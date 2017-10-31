package Tukang::AQ::Manager;

use common::sense;

use base qw(Class::Accessor::Fast);

use Types::Standard qw(Str Dict Optional Any slurpy ArrayRef HasMethods Bool Tuple HashRef RegexpRef slurpy);
use Type::Params ();

use Carp qw(carp croak);
use List::MoreUtils qw(uniq last_value);
use Akar::DBI::Statement qw(sql_param sql sql_param_inout sql_and sql_join);
use Interpolation 'E' => 'eval', 'sqlp' => sub { return sql_param(@_) };
use File::Basename qw(dirname basename);
use File::Path qw(mkpath);
use File::Spec;
use List::Util qw(sum);
use Data::Dump qw(pp);
use Sys::Hostname;
use Time::HiRes qw(gettimeofday tv_interval);

use Class::Load;

use Tukang::AQ::Constants qw(LISTEN_TIMEOUT_ERRNO DEQUEUE_TIMEOUT_ERRNO NO_MESSAGE_IN_QUEUE_ERRNO);
use Ref::Util qw(is_hashref);

use Tukang::AQ::Util qw(dbh_err wait_until);

__PACKAGE__->mk_ro_accessors(
    'dc_payload_type',
    'dc_queue',
    'listener_table',
    'queue_table',
    'listen_timeout',
    'exit_when_timeout',
    'logdir', 'logpath',
    'multi_consumer',
    'sort_order',
    'payload_type',
    'no_fork',
    'control_context',
    'queue_table_owner',
    'queue_table_name',
    'message_class',
    'storage',
    'controller_options',
);

my $new_args = Type::Params::compile(
    Str,
    slurpy Dict [
        queue_table        => Str,
        message_class      => Any,
        logdir             => Any,
        storage            => HasMethods ['dbh'],
        no_fork            => Optional [Bool],
        control_context    => Str,
        controller_options => Optional [
            HashRef | ArrayRef [ Tuple [ RegexpRef | Str, HashRef ] ]
        ],
        slurpy Any
    ]
);

sub new {
    my ( $package, $params) = $new_args->(@_);

    my $queue_table   = $params->{queue_table};
    my $message_class = $params->{message_class};
    my $logdir        = $params->{'logdir'};

    # requiring message class
    Class::Load::load_class($message_class);

    my $storage = $params->{storage};
    my $queue_table_info = _queue_table_info( $storage->dbh, $queue_table, );
    my ( $queue_table_owner, $queue_table_name ) = split /\./, $queue_table;

    my $control_context = $params->{control_context};

    return $package->SUPER::new(
        {   'listen_timeout' => 60,
            %$params,
            message_class     => $message_class,
            queue_table       => $queue_table,
            queue_table_owner => $queue_table_owner,
            queue_table_name  => $queue_table_name,
            logpath           => File::Spec->rel2abs(
                'listener-' . $queue_table . '.log', $logdir
            ),
            listener_table  => "$control_context.aq_listener",
            dc_queue        => uc("$control_context.AQ_CONTROLLER_DC_Q"),
            dc_payload_type => "$control_context.aq_controller_dc_t",
            control_context => $control_context,
            multi_consumer  => $queue_table_info->{multi_consumer},
            sort_order      => $queue_table_info->{sort_order},
            payload_type    => $queue_table_info->{object_type},
            controller_options => $params->{controller_options},
        }
    );
}

# returns 1 if the queue table is multi consumer queue
sub _queue_table_info {
    my ( $dbh, $queue_table ) = @_;

    my ( $owner, $name ) = split /\./, $queue_table;
    my $sth = $dbh->prepare(
        <<'END_SELECT'
            SELECT recipients, object_type, sort_order
            FROM sys.all_queue_tables
            WHERE owner = ? AND queue_table = ?
END_SELECT
    );
    $sth->execute( $owner, $name );
    my ( $recipients, $object_type, $sort_order ) = $sth->fetchrow_array
        or die "No record for queue table $queue_table in system catalog";
    my $multi_consumer
        = $recipients eq 'SINGLE'   ? 0
        : $recipients eq 'MULTIPLE' ? 1
        : die
        "Unexpected value of recipients ('$recipients') for queue table $queue_table";
    return {
        multi_consumer => $multi_consumer,
        object_type    => $object_type,
        sort_order     => $sort_order,
    };
}


sub queue_can_be_listened {
    my ($this, $queue) = @_;

    return !$queue->{is_exception_queue}                # normal_queue
        && $queue->{dequeue_enabled}    # started queue
        && $queue->{auto_start}                        # auto start must be set
        && !$this->load_daemon($queue); # no daemon
}

# returns queue with the message
sub listen {
    my ( $this, $agent_name ) = @_;

    my $dc_queue = $this->dc_queue;

    $this->clean_dead_daemons();

    my @to_listen = grep { $this->queue_can_be_listened($_) } $this->list_queues;

    warn
        "Listening on data queues $E{ join(', ', map {$_->{name}} @to_listen) }\n";

    my %queue_for = map { ($_->{qname} => $_); } @to_listen;
    my @agents_psql = do {
        my $idx = 0;
        map {
            ++$idx;
            <<"END_PSQL"
            l_agent_list($idx) := sys.aq\$_agent(
                null,
                $sqlp{ $_->{qname} },
                null);
END_PSQL
        } @to_listen;
    };

    my $queue_selected;
    my $listen_psql = sql(<<"END_PSQL");
        DECLARE
            l_agent_list sys.dbms_aq.aq\$_agent_list_t;
            l_agent_selected sys.aq\$_agent;
        BEGIN
            -- daemon control queue
            l_agent_list(0) := sys.aq\$_agent(
                $E{ $agent_name ? sql_param($agent_name): 'null'},
                $sqlp{ $dc_queue },
                null);
            $E{ sql_join '', @agents_psql}
            sys.dbms_aq.listen(
                agent_list => l_agent_list,
                wait       => $sqlp{ $this->listen_timeout },
                agent      => l_agent_selected
            );
            -- data queue
            $E{ sql_param_inout(\$queue_selected, 128) } := l_agent_selected.address;
        END;
END_PSQL

    my $sth = $this->dbh->prepare($listen_psql);

# 2004-08-16 daniel - locally disabled PrintError causes empty sth->err in case of error
# bug? misunderstanding?
# local $$sth{'PrintError'} = 0;
    local $SIG{__WARN__} = sub { };
    $sth->execute;

    $queue_selected =~ s/"//g;
    return if uc($queue_selected) eq uc($dc_queue);    # control queue

    my $queue = $queue_for{$queue_selected}
        or die "Invalid listen";
    return $queue;
}

# refresh
sub listener_loop {
    my ( $this, $agent_name ) = @_;

    my $exit_reason;
LISTENER_LOOP: while ( !$exit_reason ) {
        my $queue_selected = eval { $this->listen($agent_name); };
        if ( my $error = $@ ) {
            my $dbh_err = dbh_err( $this->dbh, $error );
            if ( $dbh_err && $dbh_err == LISTEN_TIMEOUT_ERRNO ) {
                warn "listen timeout\n";
                $exit_reason = 'Exit when timeout'
                    if $this->exit_when_timeout;
            }
            else {

                # Internal error
                die $error;
            }
        }
        elsif ( !$queue_selected ) {

            # dequeueing daemon control data
            my $dc_message;
            $this->txn_do(
                sub {
                    $dc_message = $this->dequeue_dc($agent_name);
                }
            );

            #handling dc
            $exit_reason = $this->handle_dc_message($dc_message);
        }
        else {
            # data queue
            $this->start_controller( $queue_selected );
        }
    }    # LISTENER_LOOP
    return $exit_reason;
}

sub handle_dc_message {
    my ( $this, $dc_payload ) = @_;

    warn 'Dequeued DC message: ' . $this->list_dc_payload($dc_payload) . "\n";
    my $action = $dc_payload->action;
    if ( $action eq 'stop' ) {

        # stop waits until daemons are finished
        return $this->handle_dc_stop();
    }
    elsif ( $action eq 'restart' ) {
        return $this->handle_dc_restart();

    }
    elsif ( $action eq 'stopped' ) {

        # this action is only to woke the listener
        # so it rearranges the queues
        warn "Wow a daemon on $E{$dc_payload->sender } stopped\n";

        # daemon stopped we continue
        return;
    }
}

sub handle_dc_stop {
    my ($this) = @_;

    # stop waits until daemons are finished
    my (%daemon_for, %queue_for);
    QUEUE: for my $queue ( $this->list_queues ) {
        my $daemon = $this->load_daemon($queue);
        if ($daemon){
            $this->stop_controller($queue, $daemon);
            $daemon_for{$queue->{qname}} = $daemon;
            $queue_for{$queue->{qname}}  = $queue;
        }
    }


    my $ok = wait_until(
        condition => sub {
            for my $qname (keys %daemon_for){
                my $queue  = $queue_for{$qname};
                my $daemon = $daemon_for{$qname};

                my $new_daemon = $this->load_daemon($queue);

                if ( !$new_daemon || $new_daemon->{id} != $daemon->{id}){
                    delete $daemon_for{$qname};
                }
            }
            return ! %daemon_for;
        },
    );
    if (!$ok){
        warn "Even after wait there are still daemons running on queues: ". join(', ', sort keys %daemon_for);
    }
    return 'stop from queue';
}

sub handle_dc_restart {
    my ($this) = @_;

    for my $queue ( $this->list_queues ) {
        my $daemon = $this->load_daemon($queue);
        if ($daemon){
            $this->stop_controller($queue, $daemon);
        }
    }

    # restart doesn't wait until daemons are finished
    return 'restart from queue';
}

sub control_listener {
    my ( $this, $action ) = @_;

    my $listener = $this->load_listener();
    if ($listener) {

        # enqueues stop into control queue
        $this->txn_do(
            sub {
                $this->enqueue_dc_action_for( $listener, $action );
            }
        );
    }
    return $listener;
}

sub load_listener {
    my ($this) = @_;

    my $listener_table = $this->listener_table;
    my $sth
        = $this->dbh->prepare(
        "SELECT id, hostname, pid, usessionid, 'l' || id AS agent_name FROM $listener_table WHERE queue_table = ?"
        );
    $sth->execute( $this->queue_table );
    return $sth->fetchrow_hashref('NAME_lc');
}

sub create_listener {
    my ($this) = @_;

    my $listener_table = $this->listener_table;
    my $control_context = $this->control_context;
    my $listener_id = $this->dbh->selectrow_array(
        "SELECT $control_context.aq_daemon_seq.nextval FROM dual"
    );
    $this->txn_do(
        sub {
            $this->dbh->do(
                <<"END_SQL"
        INSERT INTO $listener_table (id, queue_table, pid, hostname)
        VALUES (?, ?, ?, ?)
END_SQL
                , {},
                $listener_id, $this->queue_table, -1, Sys::Hostname::hostname(),
            );
        }
    );
    return $this->load_listener();
}

sub _claim_listener {
    my ($this) = @_;

    my $listener_table = $this->listener_table;

    # creates the daemon - must be done AFTER fork
    my $usessionid = $this->dbh->selectrow_array(<<"END_SELECT");
        SELECT DBMS_SESSION.UNIQUE_SESSION_ID
        FROM dual
END_SELECT
    my $done;
    $this->txn_do(
        sub {
            $done = $this->dbh->do(
                <<"END_SQL"
        UPDATE $listener_table SET usessionid = ?, pid = ?, started = SYSDATE
        WHERE queue_table = ?
END_SQL
                , {}, $usessionid, $$, $this->queue_table
            );
        }
    );
}

# stops the listener
sub stop_listener {
    my ( $this) = @_;

    # listener object probably should not be remembered
    # so it could be found disappear when deleted by running listener
    my $listener = $this->control_listener('stop');
    if ( !$listener ) {
        warn "There is no $E{$this->queue_table} listener running\n";
        return;
    }

    wait_until(
        condition => sub {

            # I wait until listener disappear with the particular id
            my $new_listener = $this->load_listener();
            return
                not( $new_listener
                && $new_listener->{id} == $listener->{id} );
        }
        )
        or die
        "Listener $E{ $this->queue_table} was sent signal but didn't stopped\n";

}

sub restart_listener {
    my ( $this ) = @_;

    # sends message to listener
    my $listener = $this->control_listener('restart');
    if ($listener) {
        wait_until(
            condition => sub {

                # I wait until listener disappear with the particular id
                my $new_listener = $this->load_listener();
                return not( $new_listener
                    && $new_listener->{id} == $listener->{id} );
            }
            )
            or die
            "Listener $E{ $this->queue_table} was sent signal but didn't stopped\n";
    }
    $this->start_listener;
}

sub wakeup_listener {
    my ($this) = @_;

    $this->control_listener('wakeup');
}

# start_queues for dequeue
sub start_queues {
    my ( $this, @queues ) = @_;

    for my $queue (@queues) {
        my $name = $queue->{name};
        if ( $queue->{dequeue_enabled} ) {
            warn
                "Queue $name already enabled for dequeue.\n";
        }
        else {
            # TO DO
            $this->start_queue($queue, 0, 1 );
            warn "Queue $name started for dequeue.\n";

            # current listener is woken up to realize queue changes
            $this->control_listener(
                $this->new_dc_payload('qstart', $name)
            );
        }
    }
}

# stops queue for dequeue
sub stop_queues {
    my ( $this, @queues ) = @_;

    my $waiter = Tukang::AQ::WaitUntil->new;
    for my $queue ( grep { $_->{dequeue_enabled} } @queues ) {
        my $name = $queue->{name};
        my $cwaiter = Tukang::AQ::WaitUntil->new;
        # TO DO
        $this->stop_queue($queue, $cwaiter);
        $waiter->add(
            'check'      => $cwaiter,
            'on_success' => sub {
                # TO DO
                # $controller->stop_queue( 0, 1 );
                warn "Queue $name stopped\n";

                # current listener is woken up to realize queue changes
                $this->control_listener(
                    $this->new_dc_payload( 'qstop', $name)
                );
            },
        );
    }
}

# start all queues for enqueue and dequeue
# to be used on fresh mirror when all queues are stopped
sub start_all_queues {
    my ($this) = @_;

    for my $queue ( $this->list_queues ) {
        my $name = $queue->{name};
        warn "Starting queue $name.\n";
        # TO DO
        # $controller->start_queue( $queue->{is_exception_queue} ? 0 : 1, 1 );
    }
}

# starts the listener
sub start_listener {
    my ( $this ) = @_;

    $this->clean_dead_listener;

    if ( my $listener = $this->load_listener ) {
        die "There is already $E{ $this->queue_table} listener running";
    }

    mkpath( $this->logdir );
    my $listener    = $this->create_listener();
    my $agent_name  = $listener->{agent_name};

    # disconnect before fork
    $this->dbh->disconnect;

    # copied from net::server::daemonize
    # i don't use net::server::daemonize directly
    # because there are no pid files and set_user needed.
    if ( !$this->no_fork ) {
        my $child_pid = fork();
        if ( $child_pid == 0 ) {
            # child
            $this->_start_listener($agent_name);
        }
        else {
            wait_until(
                sub {
                    # listener either disappear or seizes the pid
                    my $listener = $this->load_listener();
                    return !$listener || $listener->{pid} == $child_pid;
                }
            ) or die "It seem that child process haven't started\n ";
        }
    }
    else {
        $this->_start_listener($agent_name);
    }

    return;
}

sub _start_listener {
    my ( $this, $agent_name ) = @_;
    ### child process will continue on
    ### close all input/output and separate
    ### from the parent process group

    # demonizuji se a presmeruji svuj vystup do logu
    $this->redirect_daemon_output( $this->logpath );
    local $SIG{__WARN__} = sub {
        warn sprintf( '%s [%s] - ', _now_str(), $$ ), @_;
    };

    # creates the daemon - must be done AFTER fork
    $this->_claim_listener();

    ### change to root dir to avoid locking a mounted file system
    ### does this mean to be chroot ?
    chdir '/' or die "can't chdir to \"/\": $!\n ";

    ### turn process into session leader, and ensure no controlling terminal
    POSIX::setsid();
    warn "START\n";
    my $exit_reason = eval { $this->listener_loop($agent_name); };
    if ($@) {
        $exit_reason = 'ERROR: ' . $@;
    }
    warn "END listener ($exit_reason)\n";

    # current listener is deleted
    $this->clean_listener();
    exit(0);
}


sub start_controller {
    my ( $this, $queue ) = @_;

    warn "Starting daemon on queue $E{ $queue->{name} }\n";

    # when daemons end the listener (it may be different instance is notified)
    my $on_end = sub {
        $this->control_listener(
            $this->new_dc_payload( 'stopped', $queue->{qname} ) );
    };

    $this->_start_controller(
        $queue,
        $this->controller_options_for($queue)
    );
}

sub controller_options_for {
    my ($this, $queue) = @_;

    return {
        logpath => $this->logdir . '/' . 'daemon.' . $queue->{name} . '.log',
        listen_timeout    => 60,
        exit_when_timeout => 5,
        stop_after        => -1,
        $this->_controller_options_for($queue)
    };
}

sub _controller_options_for {
    my ( $this, $queue ) = @_;

    if ( is_hashref( $this->controller_options ) ) {
        return %{ $this->controller_options };
    }
    elsif ( is_arryref( $this->controller_options ) ) {
        my $queue_name = $queue->{name};

        # combination of all controller options conforming the re
        my %result = map {
            my ( $queue_re, $options ) = @{$_};

            $queue_name =~ /^(?:$queue_re)$/ ? %{$options} : ();
        } @{ $this->controller_options };

        return \%result;
    }
    return;
}

# starts daemon
sub _start_controller {
    my ( $this, $queue, $options ) = @_;

    # retrieves daemons already running on queue
    if ( my $daemon = $this->load_daemon()){
        die "There is already a daemon(s) (pid "
            . join( ', ', map { $_->pid } $daemon )
            . ') on queue '
            . $queue->{qname};
    }

    my $daemon;
    $this->storage->txn_do(
        sub {
            $daemon = $this->create_daemon( $queue );
        }
    );

    # disconnect before fork
    #$this->dbh->disconnect;

    # copied from net::server::daemonize
    # i don't use net::server::daemonize directly
    # because there are no pid files and set_user needed.

    # parent doesn't reap the children
    $SIG{'CHLD'} = 'IGNORE';
    my $child_pid = fork();
    if ( $child_pid == 0 ) {
        # child should reap the children
        $SIG{'CHLD'} = '';
        # first of all I have to use another db_Main handle
        $this->dbh->{'InactiveDestroy'} = 1;

        # this weird reconnect / disconnect
        # old controller instances can reconnect
        # the new ones (based on DBIx::Class::Storage)
        # can disconnect only, but the reconnect is accomplished
        # by following txn_do
        #
        # 2009-07-09 danielr
        # bitter experience (2 hours burned)
        # I have to use UNIVERSAL::can because DBI::db can
        # is redefined and returns false even when dbh can reconnect
        my $method = $this->dbh->UNIVERSAL::can('reconnect')? 'reconnect': 'disconnect';
        $this->dbh->$method;

        # 2008-03-11 danielr I change my name so I will be easily identified
        $0 = basename($0) . ' on ' . $queue->{qname};


        ### child process will continue on
        ### close all input/output and separate
        ### from the parent process group

        # demonizuji se a presmeruji svuj vystup do logu
        $this->redirect_daemon_output($options->{logpath});
        local $SIG{__WARN__} = sub {
            warn sprintf( '%s [%s] - ', _now_str(), $$ ), @_;
        };

        $this->storage->txn_do(
            sub {
                $this->update_daemon($daemon);
            }
        );

        ### change to root dir to avoid locking a mounted file system
        ### does this mean to be chroot ?
        chdir '/' or die "can't chdir to \"/\": $!\n ";

        ### turn process into session leader, and ensure no controlling terminal
        POSIX::setsid();

        warn "START\n";
        my $exit_reason = eval {
            $this->controller_loop( $queue, $daemon, $options );
        };
        if ($@){
            $exit_reason = 'ERROR: '. $@;
        }
        warn "END ($exit_reason)\n";

        $this->storage->txn_do(
            sub {
                $this->delete_daemon($daemon);
                if ( my $on_end = $options->{'on_end'} ) {
                    $on_end->();
                }
            }
        );
        exit(0);
    }
    else {
        # parent is waiting until daemon either disappear or its pid is son's pid
        wait_until(
            condition => sub {
                my $new_daemon = $this->load_daemon($queue);
                return !$new_daemon || $new_daemon->{pid} == $child_pid;
            }
        );
    }

    return;
}

sub do_move_messages {
    my ( $this, @condition ) = @_;
}

sub do_list_messages {
    my ( $this, $selection, $options, @queues ) = @_;

    for my $queue (@queues){
        $this->list_messages($queue, $selection, $options );
    }
}

# zobrazi jednu frontu
sub list_messages {
    my ( $this, $queue, $selection, $options ) = @_;

    $options ||= {};
    my ( $displays_empty_list, $extended_view ) = @{$options}{qw(displays_empty_list extended_view)};

    my $multi_consumer = $this->multi_consumer;
    my $message_class = $this->message_class;
    my $header =
        sprintf( "Messages in queue %s (%s)\n\n",
            $queue->{name}, $queue->{queue_type} )
            . sprintf(
            "%-32s"
                . ( $multi_consumer ? ' %-16s ' : ' ' )
                . "%2s %3s %20s %s\n",
            'Msgid', ( $multi_consumer ? 'Consumer' : () ),
            'St', 'Pri', 'Delay', $message_class->list_headers($extended_view)
            );
    my $header_displayed;

    # displays_empty_list - list messages even if there is no message
    if ($displays_empty_list) {
        print $header;
        $header_displayed = 1;
    }

    my @field_names = $message_class->payload_fields;
    my $payload_fields_sql = join(
        ', ',
        map {
            my $field_value;
            if ($message_class->payload_field_properties->{$_}{'type'}) {
                if ($message_class->payload_field_properties->{$_}{'type'} eq 'XML') {
                    $field_value = $_ . '.getClobVal() AS ' . $_;
                }
                else {
                    die "Not valid type '$_' for payload field!";
                }
            }
            else {
                $field_value = $_;
            }

            'qt.user_data. ' . $field_value;
        } @field_names
    );

    # sort order is ENQUEUE_TIME but there is enq_time in queue table
    my $order_by    = $this->sort_order;
    $order_by       =~ s/ENQUEUE_TIME/enq_time/g;

	my $consumer_sql = $multi_consumer? "qt.consumer_name": "null as consumer_name";
    my $sth         = $this->dbh->prepare(<<"END_SELECT");
        SELECT
            qt.msgid,
            qt.state,
            qt.priority,
            TO_CHAR(qt.delay, 'YYYY-MM-DD HH24:MI:SS') delay,
			$consumer_sql,
            $payload_fields_sql
        FROM
            $E{ $this->ext_queue_table } qt
        WHERE
            q_name = $sqlp{$queue->{name}}
            AND $E{ $message_class->selection_sql($selection) }
        ORDER BY
            $order_by
END_SELECT
    $sth->execute;

    my $listed = 0;
    while ( my ( $msgid, $state, $priority, $delay, $consumer_name, @fields ) = $sth->fetchrow_array ) {
        my %item;
        @item{ @field_names } = @fields;
        my $message = $message_class->new( \%item );

        # header is displayed before first message
        if ( !$header_displayed ) {
            print $header;
            $header_displayed = 1;
        }
        printf "%-32s". ($multi_consumer? ' %-16s ': ' ') ."%2s %3s %20s %s\n",
            defined($msgid) ? $msgid : '-',
            ($multi_consumer?
                (defined($consumer_name) ? $consumer_name : '-'): ()),
            defined($state) ? $state : '-',
            defined($priority) ? $priority : '-',
            defined($delay) ? $delay : '-',
            $message->list_payload($extended_view)
        ;
        $listed++;
    }

    if ($header_displayed) {
        print "\n$listed message(s) listed\n";
    }
}


sub session_is_alive {
    my ($this, $usessionid) = @_;

    my $is_alive;
    my $is_alive_sqlp = sql_param_inout( \$is_alive, 20 );
    $this->dbh->do(<<"END_PSQL");
        BEGIN
            if DBMS_SESSION.IS_SESSION_ALIVE($sqlp{ $usessionid}) then
                $is_alive_sqlp := 1;
            else
                $is_alive_sqlp := 0;
            end if;
        END;
END_PSQL
    return $is_alive;
}

sub clean_dead_listener {
    my ($this) = @_;

    my $listener = $this->load_listener()
        or return;

    my $usessionid = $listener->{usessionid};
    if ( not( $usessionid && $this->session_is_alive($usessionid) ) ) {
        my $queue_table = $this->queue_table;
        warn("Listener $queue_table seems dead\n ");
        $this->clean_listener();
    }
}

sub clean_dead_daemons {
    my ($this) = @_;

    for my $queue ($this->list_queues){
        my $daemon = $this->load_daemon($queue) or next;

        my $usessionid = $daemon->{usessionid};
        if ( $usessionid && !$this->session_is_alive($usessionid) ) {
            warn("Daemon for $queue->{qname} seems dead\n ");
            $this->delete_daemon($daemon);
        }
    }
}

sub clean_listener {
    my ($this) = @_;

    my $listener_table = $this->listener_table;
    $this->txn_do(
        sub {

            $this->dbh->do(
                "DELETE FROM $listener_table WHERE queue_table = ?",
                undef, $this->queue_table );
        }
    );
}

# enqueue the message into daemon control queue, returns the message id
{
    my $package = __PACKAGE__;

    package #
        Tukang::AQ::DaemonControl::DCPayload;
    use base qw(Class::Accessor::Fast);

    __PACKAGE__->mk_accessors( $package->dc_payload_fields );
}

sub dc_payload_fields { return qw(action sender); }

sub new_dc_payload {
    my ( $this, $action, $sender ) = @_;

    return Tukang::AQ::DaemonControl::DCPayload->new(
        {   'action' => $action,
            'sender' => $sender,
        }
    );
}

sub list_dc_payload {
    my ( $this, $payload ) = @_;

    return $payload->action
        . ( $payload->sender ? ' from ' . $payload->sender : '' );
}

# enqueue the message into daemon control queue, returns the message id
sub enqueue_dc_action_for {
    my ( $this, $daemon, $dc_payload ) = @_;

    if ( !ref $dc_payload ) {
        return $this->enqueue_dc_action_for( $daemon,
            $this->new_dc_payload($dc_payload) );
    }

    my $agent_name = $daemon->{agent_name};
    my $compose_psql = sql_join ', ',
        map { sql_param( $dc_payload->$_ ) } $this->dc_payload_fields;

    my $msgid;
    my $dc_payload_type   = $this->dc_payload_type;
    my $dc_queue          = $this->dc_queue;
    $this->dbh->do(<<"END_PSQL");
        DECLARE
            l_message_properties    sys.dbms_aq.message_properties_t;
            l_enqueue_options       sys.dbms_aq.enqueue_options_t;
            l_msgid raw(128);
            l_payload $dc_payload_type := $dc_payload_type($compose_psql);
        BEGIN
            -- only multiple consumer has to set rcpt
            l_message_properties.recipient_list(0) := sys.aq\$_agent(
                $sqlp{ $agent_name },
                $sqlp{ $dc_queue },
                0);
            sys.dbms_aq.enqueue(
                queue_name  => $sqlp{ $dc_queue },
                enqueue_options => l_enqueue_options,
                message_properties => l_message_properties,
                payload            => l_payload,
                msgid              => l_msgid);
            $E{ sql_param_inout(\$msgid, 128) } := l_msgid;
        END;
END_PSQL
    return $msgid;
}

sub dequeue_dc {
    my ( $this, $agent_name ) = @_;

    # decomposing the variable into params
    my %dc_payload;

    my $consumer_name = $agent_name;
    my $dc_queue      = $this->dc_queue;
    my $sql           = sql(<<"END_PSQL");
        DECLARE
            l_message_properties    sys.dbms_aq.message_properties_t;
            l_dequeue_options       sys.dbms_aq.dequeue_options_t;
            l_payload               $E{ $this->dc_payload_type };
            l_target_queue varchar2(256);
            l_msgid raw(128);
        BEGIN
            l_dequeue_options.navigation    := sys.dbms_aq.FIRST_MESSAGE;
            l_dequeue_options.wait          := 1;
            l_dequeue_options.dequeue_mode  := sys.dbms_aq.REMOVE;
            l_dequeue_options.consumer_name := $E{ $consumer_name ?  sql_param($consumer_name): 'null' };
            sys.dbms_aq.dequeue(
                queue_name  => $sqlp{ $dc_queue },
                payload     => l_payload,
                message_properties => l_message_properties,
                dequeue_options    => l_dequeue_options,
                msgid              => l_msgid);
            $E{
                sql_join "\n", map {
                    my $param = sql_param_inout( \$dc_payload{$_}, 48 );
                    sql("$param := l_payload.$_;");
                } $this->dc_payload_fields
            }
        END;
END_PSQL
    my $sth = $this->dbh->prepare($sql);
    $sth->execute;
    return $this->new_dc_payload( @dc_payload{qw(action sender)} );
}

sub redirect_daemon_output {
    my ($this, $logfile) = @_;

    my $logfh = FileHandle->new( $logfile, '>>' )
        or die "Can't open '$logfile' for writing: $! \n ";

    warn "Output will be redirected to '$logfile'\n";

    open STDIN, '</dev/null' or die "Can't open STDIN from /dev/null: $!\n ";
    open STDOUT, '>&' . $logfh->fileno()
        or die "Can't reopen STDOUT into log '$logfile': $!\n ";
    STDOUT->autoflush(1);
    open STDERR, '>&STDOUT' or die "Can't reopen STDERR into STDOUT: $!\n ";
}

sub list_queues {
    my ($this) = @_;

    my $control_context = $this->control_context;
    my $sth = $this->dbh->prepare(<<"END_SQL");
        SELECT owner, name, queue_table, qid, queue_type, max_retries, retry_delay,
            enqueue_enabled AS x_enqueue_enabled,
            dequeue_enabled AS x_dequeue_enabled,
            retention,
            NVL(aq_queue.auto_start, 1) AS auto_start
        FROM sys.all_queues
            LEFT OUTER JOIN $control_context.aq_queue
                ON $control_context.aq_queue.queue = all_queues.owner || '.' || all_queues.name
        WHERE owner = ?
            AND queue_table = ?
        ORDER BY DECODE(queue_type, 'NORMAL_QUEUE', 1, 'EXCEPTION_QUEUE', 2, 3), qid
END_SQL

    $sth->execute( $this->queue_table_owner, $this->queue_table_name);
    my @queues;
    while( my $hr = $sth->fetchrow_hashref('NAME_lc')){
        push @queues, +{
            %$hr,
            dequeue_enabled => ($hr->{x_dequeue_enabled} =~ /yes/i? 1: 0),
            enqueue_enabled => ($hr->{x_enqueue_enabled} =~ /yes/i? 1: 0),
            qname => join('.', $hr->{owner}, $hr->{name}),
            is_exception_queue => ($hr->{queue_type} =~ /EXCEPTION_QUEUE/?  1:0),
        };
    }

    return @queues;
}

sub load_daemon {
    my ($this, $queue) = @_;

    my $control_context = $this->control_context;
    my $sth = $this->dbh->prepare(<<"END_SQL");
        SELECT id, daemon_num, usessionid, pid, started, being_stopped, 'd' || id AS agent_name
        FROM $control_context.aq_daemon
        WHERE queue = ?
END_SQL
    $sth->execute( $queue->{qname} );
    return $sth->fetchrow_hashref('NAME_lc');
}

sub create_daemon {
    my ($this, $queue) = @_;

    my $control_context = $this->control_context;
    my $sth = $this->dbh->prepare(<<"END_SQL");
        INSERT INTO $control_context.aq_daemon (id, queue, daemon_num, pid)
        VALUES ($control_context.aq_daemon_seq.nextval, ?, 1, -1)
END_SQL
    $sth->execute( $queue->{qname} );
    return $this->load_daemon($queue);
}

sub update_daemon {
    my ( $this, $daemon ) = @_;

    # creates the daemon - must be done AFTER fork
    my $usessionid = $this->dbh->selectrow_array(<<"END_SELECT");
        SELECT DBMS_SESSION.UNIQUE_SESSION_ID
        FROM dual
END_SELECT

    my $control_context = $this->control_context;
    my $sth             = $this->dbh->prepare(<<"END_SQL");
        UPDATE $control_context.aq_daemon
        SET pid = ?, usessionid = ?, started = sysdate
        WHERE id = ?
END_SQL
    $sth->execute( $$, $usessionid, $daemon->{id} );
}

sub delete_daemon {
    my ( $this, $daemon ) = @_;

    # creates the daemon - must be done AFTER fork
    my $control_context = $this->control_context;
    my $sth             = $this->dbh->prepare(<<"END_SQL");
        DELETE FROM $control_context.aq_daemon
        WHERE id = ?
END_SQL
    $sth->execute( $daemon->{id} );
}

sub ext_queue_table {
    my $this = shift;

    if ( $this->multi_consumer ) {
        my $qv = my $qt = $this->queue_table;
        $qv =~ s/(?<=\.)/AQ\$/;

        return <<"END_SELECT";
(
select qt.*, qv.consumer_name
from $qt qt join $qv qv on qt.msgid = qv.msg_id
)
END_SELECT

    }
    return $this->queue_table;
}

sub move_messages {
    my ( $this, $queue, $selection, $target_queue, $options ) = @_;

    my $name = $queue->{name};
    $queue->{dequeue_enabled}
        or die <<"END_ERROR";
Queue $name is not started for dequeue,
no messages can be moved from it.
END_ERROR

    my $target_queue_decl;
    my $target_queue_psql;
    if ($target_queue) {
        # target_queue must be qualified name
        if ( $target_queue !~ /\./ ) {
            $target_queue = $this->queue_table_owner . '.' . $target_queue;
        }
        $target_queue_psql = sql_param($target_queue);
    }
    else {

        # $this->message_class->target_queue_sql returns
        #   either (decl, expression)
        #   or    (expression)
        ( $target_queue_psql, $target_queue_decl )
            = reverse $this->message_class->target_queue_sql(
            $this,
            {   'payload'            => 'l_payload',
                'message_properties' => 'l_message_properties',
                'msgid'              => 'l_msgid',
            }
            );
    }

    # something can be done when the message is moved from queue to queue
    # EXPERIMENTAL
    my $on_move_psql = $this->message_class->on_move_psql(
        $this,
        {   'payload'            => 'l_payload',
            'message_properties' => 'l_message_properties',
            'msgid'              => 'l_msgid',
        }
    );
    if (!defined $on_move_psql){
        $on_move_psql = '';
    }

    my $mc_dequeue = '';
    my $mc_enqueue = '';
    if ( $this->multi_consumer ) {
        $mc_dequeue = <<'END_MC_DEQUEUE';
        l_dequeue_options.consumer_name := l_msgid_selection.consumer_name;
END_MC_DEQUEUE

        $mc_enqueue = <<'END_MC_ENQUEUE';
        l_message_properties.recipient_list(0) := sys.aq$_agent(l_msgid_selection.consumer_name, null, 0);
END_MC_ENQUEUE

    }

    my $body_psql = sql(<<"END_PSQL");
        -- dequeue part
        l_dequeue_options.msgid := l_msgid_selection.msgid;
        l_dequeue_options.wait  := sys.dbms_aq.NO_WAIT;
        $mc_dequeue

        sys.dbms_aq.dequeue(
            queue_name  => $sqlp{ $name },
            payload     => l_payload,
            message_properties => l_message_properties,
            dequeue_options    => l_dequeue_options,
            msgid              => l_msgid);

        -- modification_part
        l_target_queue := $target_queue_psql;
        if l_enqueued_into.EXISTS(l_target_queue) then
            l_enqueued_into(l_target_queue) := l_enqueued_into(l_target_queue) + 1;
        else
            l_enqueued_into(l_target_queue) := 1;
        end if;

        -- 2008-03-25 danielr
        -- I cannot leave delay as it is or the messages would be shifted
        -- to future
        -- if delay is NOT dbms_aq.NO_WAIT then it is set
        -- to remaining number of seconds to the desired dequeue time
        -- If this time already expired then delay is set to dbms_aq.NO_WAIT
        l_delay  := l_message_properties.delay;
        if not((sys.dbms_aq.NO_WAIT is null and l_delay is null)
           or l_delay = sys.dbms_aq.NO_WAIT)
        then
            l_delay := l_delay - round(86400 * (sysdate - l_message_properties.enqueue_time));
            if l_delay <= 0 then
                l_delay := sys.dbms_aq.NO_WAIT;
            end if;
            l_message_properties.delay := l_delay;
        end if;

        -- something may be done when the message is moved from queue to queue
        $on_move_psql

        $mc_enqueue

        -- enqueue part
        sys.dbms_aq.enqueue(
            queue_name  => l_target_queue,
            enqueue_options => l_enqueue_options,
            message_properties => l_message_properties,
            payload            => l_payload,
            msgid              => l_msgid);
END_PSQL

    my $where_psql = sql_and( "q_name = $sqlp{$queue->{name}}",
        $this->message_class->selection_sql($selection) );

    my $dequeued;
    my $enqueued_into_str;
    my $the_columns = join ', ', map {"qt.$_"} 'msgid',
        $this->multi_consumer ? 'consumer_name' : ();
    my $moving_psql = sql(<<"END_PSQL");
        DECLARE
            -- target distribution mapping queue => messages_moved
            TYPE enqueued_into_type IS TABLE OF NUMBER INDEX BY VARCHAR2(64);
            l_queue                 varchar2(64);
            l_enqueued_into         enqueued_into_type;
            l_enqueued_into_str     varchar2(2048);
            l_message_properties    sys.dbms_aq.message_properties_t;
            l_dequeue_options       sys.dbms_aq.dequeue_options_t;
            l_dequeued              number default 0;
            l_enqueue_options       sys.dbms_aq.enqueue_options_t;
            l_payload $E{ $this->payload_type };
            l_target_queue varchar2(256);
            l_msgid raw(128);
            l_delay binary_integer; -- sys.dbms_aq.message_properties_t.delay%TYPE;
            $E{ $target_queue_decl || '-- no target_queue_psql declaration' }
        BEGIN
            for l_msgid_selection in (
                SELECT $the_columns
                FROM $E{ $this->ext_queue_table } qt
                WHERE $where_psql
            )
            LOOP
                $body_psql
                l_dequeued := l_dequeued + 1;
            END LOOP;
            $E{ sql_param_inout( \$dequeued, 20 ) } := l_dequeued;

            -- passing the distribution back to Perl
            l_queue := l_enqueued_into.FIRST;
            l_enqueued_into_str := '';
            loop
                exit when l_queue is null;
                l_enqueued_into_str := l_queue || ':' || l_enqueued_into(l_queue) || ':';
                l_queue := l_enqueued_into.NEXT(l_queue);
            end loop;
            $E{ sql_param_inout(\$enqueued_into_str, 2050) } := l_enqueued_into_str;
        END;
END_PSQL
    $this->storage->txn_do(
        sub {
            $this->dbh->do($moving_psql);
        }
    );

    if ($enqueued_into_str) {
        my %enqueued_into = $enqueued_into_str =~ m{(.*?):}g;
        for my $queue ( keys %enqueued_into ) {
            warn sprintf "  %s enqueued into %s\n", $enqueued_into{$queue},
                $queue;
        }
    }
}

sub remove_messages {
    my ( $this, $queue, $selection, $options ) = @_;

    my $name = $queue->{name};
    $this->retrieve_queue->dequeue_enabled
        or die <<"END_ERROR";
Queue $name is not started for dequeue,
no messages can be removed from it.
END_ERROR

    my $mc_dequeue
        = $this->multi_consumer
        ? 'l_dequeue_options.consumer_name := l_msgid_selection.consumer_name;'
        : '';
    my $body_psql = sql(<<"END_PSQL");
        -- dequeue part
    	$mc_dequeue
        l_dequeue_options.msgid := l_msgid_selection.msgid;
        l_dequeue_options.wait  := sys.dbms_aq.NO_WAIT;
        $mc_dequeue
        sys.dbms_aq.dequeue(
            queue_name  => $sqlp{ $name },
            payload     => l_payload,
            message_properties => l_message_properties,
            dequeue_options    => l_dequeue_options,
            msgid              => l_msgid);
END_PSQL

    my $where_psql = sql_and( "q_name = $sqlp{$name}",
        $this->message_class->selection_sql($selection) );

    my $dequeued;
    my $the_columns = join ', ', map {"qt.$_"} 'msgid',
        $this->multi_consumer ? "consumer_name" : ();
    my $moving_psql = sql(<<"END_PSQL");
        DECLARE
            l_queue                 varchar2(64);
            l_message_properties    sys.dbms_aq.message_properties_t;
            l_dequeue_options       sys.dbms_aq.dequeue_options_t;
            l_dequeued              number default 0;
            l_payload $E{ $this->payload_type };
            l_msgid raw(128);
            l_delay binary_integer; -- sys.dbms_aq.message_properties_t.delay%TYPE;
        BEGIN
            for l_msgid_selection in (
                SELECT $the_columns
                FROM $E{ $this->ext_queue_table } qt
                WHERE $where_psql
            )
            LOOP
                $body_psql
                l_dequeued := l_dequeued + 1;
            END LOOP;
            $E{ sql_param_inout( \$dequeued, 20 ) } := l_dequeued;
        END;
END_PSQL
    $this->storage->txn_do(
        sub {
            $this->dbh->do($moving_psql);
        }
    );

    warn "$dequeued message(s) removed from " . $this->queue . "\n";
}


sub set_auto_start {
    my ( $this, $queue, $auto_start ) = @_;

    my $control_context = $this->control_context;
    my $queue_qname = $queue->{qname};
    $this->storage->txn_do(
        sub {
            $this->_assert_aq_queue( $queue_qname);
            my $updated = $this->dbh->do(
                <<"END_SQL",
            UPDATE $control_context.aq_queue
            SET auto_start = ?
            WHERE queue = ?
END_SQL
                , {}, $auto_start, $queue_qname
            );
        }
    );
}

sub _assert_aq_queue {
    my ( $this, $queue_qname) = @_;

    my $table = join('.', $this->control_context, 'aq_queue');
    my $exists = $this->dbh->selectrow_array(
        'SELECT 1 FROM $table WHERE queue = ?',
        {}, $queue_qname
    );
    if ( !$exists ) {
        $this->dbh->do( 'INSERT INTO $table (queue) VALUES (?)',
            {}, $$queue_qname );
    }
}

# enqueues stop message into control queue,
# returns subroutine checking whether the daemon was stopped
sub stop_controller {
    my ( $this, $queue, $daemon  ) = @_;

    # enqueue stop into daemon queue
    my $name = $queue->{name};
    if ( $daemon->{being_stopped} ) {
        warn "Daemon on $name is already being stopped\n";
        return;
    }

    warn "Stopping daemon on $name\n";
    $this->storage->txn_do(
        sub {
            $this->enqueue_dc_action_for( $daemon, 'stop' );
            my $control_context = $this->control_context ;
            $this->dbh->do(
                "
                    UPDATE $control_context.aq_daemon SET being_stopped = sysdate
                    WHERE id = ?
                ",
                undef,
                $daemon->{id}
            );
        }
    );

}

sub controller_loop {
    my ($this, $queue, $daemon, $options) = @_;

    my $listen_sth
        = $this->prepare_controller_listen( $queue, $daemon, $options );
    my $dequeue_sth = $this->prepare_controller_dequeue( $queue, $options );
    my $timeouts       = 0; # number of timeout in a row occured so far
    my $dequeued = 0;
    my $errors   = 0;

    my $stop_after        = $options->{stop_after};
    my $exit_when_timeout = $options->{exit_when_timeout};

DAEMON_LOOP:
    while (1) {
        my $dc_queue_selected = eval {

# 2004-08-16 daniel - locally disabled PrintError causes empty sth->err in case of error
# bug? misunderstanding?
# local $$sth{'PrintError'} = 0;
            local $SIG{__WARN__} = sub { };
            $listen_sth->();
        };
        if ( my $error = $@ ) {
            if ( dbh_err($this->dbh, $error) == LISTEN_TIMEOUT_ERRNO ) {
                $timeouts++;
                return "timeout no. $timeouts"
                    if $exit_when_timeout && $timeouts >= $exit_when_timeout;
                next DAEMON_LOOP;
            }
            else {

                # Internal error
                die $error;
            }
        }

        # timeout counter can be reset
        $timeouts = 0;

        # dequeueing daemon control data
        if ($dc_queue_selected) {
            my $dc_message;
            $this->storage->txn_do(
                sub {
                    $dc_message = $this->dequeue_dc( $daemon->{agent_name} );
                }
            );

            # daemon control - the only reaction is stopped
            return $dc_message->action . ' from queue';
        }

        # dequeueing data
        my ($message, $message_properties, $msgid);
        eval {
            $this->storage->txn_do(
                sub {
                    ( $message, $message_properties, $msgid ) = $dequeue_sth->();

                    my $attempts = $message_properties->{'attempts'};
                    my $msg = $message->list_payload
                        . ( $attempts ? ", retry " . $attempts : '' ) ;
                    warn "Dequeued $msg\n";

                    $dequeued ++;

                    # here is the message handling
                    my @started = gettimeofday();
                    $this->handle_message( $message, $message_properties, $msgid );
                    my $elapsed = tv_interval(\@started);
                    warn sprintf "finished %s, took %fs (%.3f/s)\n",
                        $msg, $elapsed, 1/$elapsed;
                }
            );
        };

        if ( my $error = $@ ) {
            # dequeue error
            if (!$message){
                # timeout is ignored, other error propagated
                next DAEMON_LOOP
                    if dbh_err($this->dbh, $error) == DEQUEUE_TIMEOUT_ERRNO;
                die $error;
            }
            # application error
            $this->handle_message_error($error, $message, $message_properties, $msgid);
        }

        if ( $stop_after > 0 && $dequeued >= $stop_after ) {
            return 'Stopped stop_after='. $stop_after;
        }
    }    # DAEMON_LOOP
}

# listen_sub returns subroutine because it has to store statement handle
sub prepare_controller_listen {
    my ( $this, $queue, $daemon, $options ) = @_;

    #my $listen_timeout =
    #    $this->exit_when_timeout
    #    ? sql('sys.dbms_aq.NO_WAIT')
    #    : sql_param( $this->listen_timeout );
    my $listen_timeout =
        defined( $options->{listen_timeout}  )
        ? sql_param( $options->{listen_timeout} )
        : sql('sys.dbms_aq.NO_WAIT');

    my $agent_name = $daemon->{agent_name};
    my $dc_queue = $this->dc_queue;
    my $queue_selected;
    my $listen_psql = sql(<<"END_PSQL");
        DECLARE
            l_agent_list sys.dbms_aq.aq\$_agent_list_t;
            l_agent_selected sys.aq\$_agent;
        BEGIN
            -- daemon control queue
            l_agent_list(0) := sys.aq\$_agent(
                $E{ $agent_name? sql_param($agent_name): 'null' },
                $sqlp{ $dc_queue },
                null);
            -- ordinary queue
            l_agent_list(1) := sys.aq\$_agent(
                null,
                $sqlp{ $queue->{qname} },
                null);
            sys.dbms_aq.listen(
                agent_list => l_agent_list,
                wait       => $listen_timeout,
                agent      => l_agent_selected);
            $E{ sql_param_inout(\$queue_selected, 128) } := l_agent_selected.address;
        END;
END_PSQL

    my $sth = $this->dbh->prepare($listen_psql);

    # this procedure calls listen and returns 1 if dc_queue was selected
    return sub {
        $sth->execute;

        # there is something strange with Oracle
        $queue_selected =~ s/"//g;
        return uc($queue_selected) eq uc( $dc_queue );
    };
}

sub prepare_controller_dequeue {
    my ( $this, $queue, $options ) = @_;

    # decomposing object the variable into params
    my (%message, %message_properties, $msgid);
    my $decompose_psql = sql_join(
        '',
        map {
            my $size = $this->message_class->payload_field_properties->{$_}{'size'} || 128;

            my $field_value;
            if ($this->message_class->payload_field_properties->{$_}{'type'}) {
                if ($this->message_class->payload_field_properties->{$_}{'type'} eq 'XML') {
                    $field_value = $_ . '.getClobVal()';
                }
                else {
                    die "Not valid type '$_' for payload field!";
                }
            }
            else {
                $field_value = $_;
            }

            sql( sql_param_inout( \$message{$_}, $size ) . ":= l_payload.$field_value;\n" );
        } $this->message_class->payload_fields
    );

    my $dequeue_navigation = $options->{dequeue_navigation} // 'NEXT_MESSAGE';
    my $dequeue_timeout = $options->{dequeue_timeout} // 1;

    # message properties are so far only attempts
    my $decompose_message_properties_psql = sql_join '', map {
        my $param = sql_param_inout( \$message_properties{$_}, 128 );
        sql("$param := l_message_properties.$_;\n");
    } qw(attempts);

    my $sql = sql(<<"END_PSQL");
        DECLARE
            l_message_properties    sys.dbms_aq.message_properties_t;
            l_dequeue_options       sys.dbms_aq.dequeue_options_t;
            l_payload               $E{ $this->payload_type };
            l_target_queue varchar2(256);
            l_msgid raw(128);
            l_delay binary_integer;
        BEGIN
            l_dequeue_options.wait          := $sqlp{ $dequeue_timeout };
            l_dequeue_options.navigation    := sys.dbms_aq.$E{ $dequeue_navigation };
            l_dequeue_options.dequeue_mode  := sys.dbms_aq.REMOVE;
            sys.dbms_aq.dequeue(
                queue_name  => $sqlp{ $queue->{qname} },
                payload     => l_payload,
                message_properties => l_message_properties,
                dequeue_options    => l_dequeue_options,
                msgid              => $E{ sql_param_inout(\$msgid, 128) }
            );
            $decompose_psql
            $decompose_message_properties_psql
        END;
END_PSQL
    my $sth = $this->dbh->prepare($sql);
    return sub {
        $sth->execute;
        my $message_obj = $this->message_class->new( \%message );
        return wantarray
            ? ( $message_obj, \%message_properties, $msgid )
            : $message_obj;
    };
}

# daemon_loop - returns the string with the reason of exit
sub handle_message {
    my ( $this, $message, $message_properties, $msgid ) = @_;

    $message->handle($message_properties, $msgid);
}

sub handle_message_error {
    my ( $this, $error, $message, $message_properties, $msgid ) = @_;
    warn "Error $error\n ";

    # the rollback here is necessary for AQ
    $message->handle_error( $error, $message_properties, $msgid );
}

##############################################################################
# Queue manipulations
##############################################################################

# clones current queue (uses its parameters)
sub clone_queue {
    my ( $this, $queue, $new_queue, $queue_params ) = @_;

    my $max_retries = $queue_params->{'max_retries'} // $queue->{max_retries};
    my $retry_delay = $queue_params->{'retry_delay'} // $queue->{retry_delay};
    $this->dbh->do(<<"END_PSQL");
        DECLARE
        BEGIN
            sys.dbms_aqadm.create_queue(
                queue_name  => $sqlp{ $new_queue },
                queue_table => $sqlp{ $this->queue_table },
                max_retries => $sqlp{ $max_retries },
                retry_delay => $sqlp{ $retry_delay }
            );
        END;
END_PSQL
}

# starts the queue
sub start_queue {
    my ( $this, $queue, $enqueue_in, $dequeue_in ) = @_;

    my $enqueue = $enqueue_in // ($queue->{is_exception_queue} ? 0 : 1);
    my $dequeue   = $dequeue_in // 1;

    my $name = $queue->{name};
    $this->dbh->do(<<"END_PSQL");
        BEGIN
            sys.dbms_aqadm.start_queue(
                $sqlp{$name},
                $E{ $enqueue? 'true': 'false'},
                $E{ $dequeue? 'true': 'false'}
            );
        END;
END_PSQL
}

sub stop_queue {
    my ( $this, $queue, $enqueue_in, $dequeue_in ) = @_;

    my $enqueue = $enqueue_in // ($queue->{is_exception_queue} ? 0 : 1);
    my $dequeue   = $dequeue_in // 1;

    my $name = $queue->{name};
    $this->dbh->do(<<"END_PSQL");
        BEGIN
            sys.dbms_aqadm.stop_queue(
                $sqlp{$name},
                $E{ $enqueue? 'true': 'false'},
                $E{ $dequeue? 'true': 'false'}
            );
        END;
END_PSQL
}

sub do_drop_queue {
    my ($this, $queue) = @_;

    die "Queue has to be stopped first before drop\n "
        if $queue->{dequeue_enabled};

    my $name = $queue->{name};
    my $message_count = $this->message_count($name);
    die "Queue has to be emptied before drop\n "
        if $message_count && grep {$_} values %{$message_count};

    $this->dbh->do(<<"END_PSQL");
        DECLARE
        BEGIN
            sys.dbms_aqadm.drop_queue(
                queue_name  => $sqlp{$queue->{qname}}
            );
        END;
END_PSQL
}

sub do_list_queues {
    my ( $this, $hide_legend ) = @_;

    my $listener = $this->load_listener();
    print $listener
        ? "Listener running on $E{ $this->queue_table } "
        . "(hostname $E{$listener->{hostname}}, pid $E{ $listener->{pid} })\n"
        : "No listener running on $E{ $this->queue_table}\n";

    print "\n";
    printf "%-30s %-10s %-3s %-3s %20s %23s\n", 'Queue', 'Type', 'Enq', 'Deq',
        'Daemon', 'Messages(Active)';
    for my $queue ( $this->list_queues ) {
        my $name               = $queue->{name};
        my $is_exception_queue = $queue->{is_exception_queue};

# if there are no messages or queue is exception queue only total number is reported
# otherwise TOTAL/ACTIVE
        my $messages_str = do {
            my $message_count    = $this->message_count($name);
            my $total_messages   = sum( values %{$message_count} ) || 0;
            my $active_messsages = $message_count->{0} || 0;
            ( !$total_messages || $is_exception_queue )
                ? $total_messages
                : "$total_messages ($active_messsages)";
        };

        # 2008-03-20
        # daemon is so far only one, but later ...
        my $daemon = $this->load_daemon($queue);
        my $pid_str
            = $daemon
            ? join( ', ', $daemon->{pid} )
            : !$queue->{dequeue_enabled} ? 'S'    # queue is stopped
            : $is_exception_queue        ? 'E'
            : !$queue->{auto_start}      ? 'O'    # auto_start is reset
            : $listener                  ? 'L'    # queue is listened
            :                              '-';
        printf "%-30s %-10s %-3s %-3s %20s %23s\n", $name,
            ( $is_exception_queue ? 'exception' : 'normal' ),
            (
              $is_exception_queue       ? '-'
            : $queue->{enqueue_enabled} ? 'yes'
            :                             'no'
            ),
            ( $queue->{dequeue_enabled} ? 'yes' : 'no' ), $pid_str,
            $messages_str;
    }
    if ( !$hide_legend ) {
        print $this->list_queues_legend;
    }
}

sub list_queues_legend {
    my ($this) = @_;

    return <<"END_LEGEND";

Columns legend:

Queue   - queue name
Type    - either normal or exception queue
Enq     - can the messages be enqueued?
Deq     - can the messages be dequeued?
Daemon  - "state" of the queue
        S   - stopped for dequeue (physically), no daemon can be run, no messages moved from
        E   - exception queue, no daemon can be run
        0   - auto_start is reset, no daemon will be run, but messages can be dequeued from
        L   - listener is listening on queue
        <number> - pid of the currently running daemon
Messages (Active) - number of messages in queue. Inactive messages
    are those to be run at particular time or failed ones waiting for next retry.
END_LEGEND
}

sub control_qname {
    my ($this, $table) = @_;
    return $this->control_context . '.' . $table;
}

sub dbh {
    my ($this) = @_;
    return $this->storage->dbh;
}

sub txn_do {
    my ( $this, $code ) = @_;
    return $this->storage->txn_do($code);
}

# returns hash STATE => COUNT
sub message_count {
    my ($this, $queue_name) = @_;

    my $sth = $this->dbh->prepare(<<"END_SQL");
        SELECT state, count(*)
        FROM $E{ $this->queue_table }
        WHERE q_name = ?
        GROUP BY state
END_SQL
    my %msgs_dist;
    $sth->execute( $queue_name );
    while ( my ( $state, $msgs ) = $sth->fetchrow_array ) {
        $msgs_dist{$state} = $msgs;
    }
    return \%msgs_dist;
}

sub _now_str {
                #  0    1    2     3     4    5     6     7     8
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =
                                                            localtime(time);
    return sprintf(
        '%04d-%02d-%02d %02d:%02d:%02d',
        $year + 1900,
        $mon + 1, $mday, $hour, $min, $sec
    );
}

1;

# vim: expandtab:shiftwidth=4:tabstop=4:softtabstop=0:textwidth=78:
