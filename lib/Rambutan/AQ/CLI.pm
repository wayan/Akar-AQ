package Rambutan::AQ::CLI;

use common::sense;

use Carp qw(carp croak);
use File::Spec;

use Rambutan::AQ::Manager;
use Akar::CLI;
use Interpolation 'E' => 'eval';

# 2008-04-02 danielr
# toto je asi jedina funkce, ktera by se mela vyuzivat
sub build_cli_for {
    my ($this, $manager) = @_;

    my $default_action_handler = sub {
        my ( $script_call, $action, $manager ) = @_;
        my $method = 'run_' . $action;
        return $this->$method( $script_call, $manager );
    };

    my $cli;
    $cli = Akar::CLI->new(
        {
            'on_run' => sub {
                my ($script_call) = @_;

                # all actions are given $manager as the last param
                for my $action ( $script_call->actions(1) ) {
                    my $action_handler = $cli->get_action($action)->on_run
                        || $default_action_handler;

                    # manager is passed as the last parameter
                    $action_handler->( $script_call, $action, $manager );
                }
                }
        }
    );

    # queue selection
    $cli->add_option_group(
        'queue_selection - Queue selection',
        [   $cli->add_option(
                'queue|q=s{Queue or RE} - queue to select the messages from, '
                    . 'may be regular expression'
            ),
            $cli->add_option(
                'exception_queue|eq - selects the exception queue'),
            $cli->add_option('all_queues|qa - selects all queues'),
        ]
    );

    # message selection
    $cli->add_option_group(
        'message_selection - Message selection',
        [   $cli->add_option('all_messages|ma - Selects all messages'),
            $cli->add_option('msgid=s - Msgid'),
            $cli->add_option(
                'any_selection|where=s - Where condition selecting the messages'
            ),
            $cli->add_option(
                'message_key|mkey=s% - Select message according to payload field'
            ),
            $cli->add_option(
                'active_message|active! - Selects active (state = 0) messages'
            ),
            ($manager->multi_consumer?
                $cli->add_option(
                    'consumer_name|consumer=s - Selects only messages for a certain consumer'
                ): ()
            )
        ]
    );

    $cli->add_option('target_queue|enqueue_into=s - Target queue');

    $cli->add_option('stop_after=i{N} - Stops after N messages is deqeueued');
    $cli->add_option(
        'exit_when_timeout|ewt! - Stops after first timeout occurs');
    $cli->add_option(
        'listen_timeout=i{SEC} - When listening timeout is set to SEC seconds'
    );

    $cli->add_option_group( 'daemon_control - Daemon controling options',
        [qw(stop_after exit_when_timeout listen_timeout)] );

    $cli->add_option(
        'extended_view|ev! - Shows messages in extended (domain specific) meaning'
    );

    $cli->add_option(
        'displays_empty_list|del! - Displays even queues with no selected messages'
    );

    $cli->add_option_group(
        'show_options - Message display options',
        [qw(extended_view displays_empty_list)]
    );

    $cli->add_option(
        'new_queue_params|nqp=s{PARAM=VALUE}% - Params of newly created queue (max_retries, retry_delay)'
    );

    # actions
    $cli->add_action('list - lists all queues');
    $cli->set_default_action('list');
    $cli->add_action('show - shows the selected queue');

    if ( !$manager->multi_consumer ) {

        # for multi_consumer queue there is currently no daemon processing
        $cli->add_action(
            'start|s - starts the manager, start to listen on all queues ');
        $cli->add_action('stop|p - stops the manager');
        $cli->add_action('restart|rs - restarts the manager');

        $cli->add_action('set_auto_start|sas  - daemon can be started ');
        $cli->add_action(
            'reset_auto_start|ras - daemon will not be started for queue');
    }


    $cli->add_action(<<"END_SAQ");
start_all_queues|SAQ - start all queues for enqueueing and dequeueing

Use this action whenever the database is freshly created (mirrored),
you may see something like:
ORA-25207: enqueue failed, queue QUEUE_NAME is disabled from enqueueing
in your logs.
END_SAQ
    $cli->add_action('start_queue|sq - start selected queues for dequeue');
    $cli->add_action('stop_queue|pq - stop selected queues for dequeue');

    $cli->add_action('start_queue_for_enqueue|seq - start selected queues for enqueue');
    $cli->add_action('stop_queue_for_enqueue|peq - stop selected queues for enqueue');


    $cli->add_action('wakeup|wk - wakeups the manager');
    $cli->add_action('move - moves selected messages into other queue');
    $cli->add_action('remove - removes selected messages from queue');
    $cli->add_action(
        'eqh - moves the selected messages from exception queue');
    $cli->add_action(
        'testrun|tr - start(s) queue, run some messages and stop(s) queue again');
    $cli->add_action('logfile - prints the path to daemon the log (s)');

    {
        my $action_qc
            = $cli->add_action('clone_queue|qc - Creates new queue');
        $cli->add_option(
            'new_queue|nq=s{NAME} - The name of newly created queue');
        $action_qc->requires_all(qw(new_queue));
    }

    $cli->add_action('drop_queue|qc - Drops the queue');

    return $cli;
}

# lists selected queues
sub run_list {
    my ( $this, $script_call, $manager ) = @_;

    $manager->do_list_queues;
}

sub run_show {
    my ( $package, $script_call, $manager ) = @_;

    $manager->do_list_messages(
        $script_call->value_of('message_selection'),
        $script_call->value_of('show_options') || {},
        $package->select_queues($manager,  $script_call )
    );
}

sub run_start {
    my ($package, $script_call, $manager) = @_;

    if ( my $queue_selection = $script_call->value_of('queue_selection') ) {
        warn <<"END_WARN";
! Attention !
Action 'start' starts whole listener, so queue selection ($E{
    join ', ', map {"-$_"} keys %{$queue_selection}
}) is useless.
For particular queue manipulation use actions 'stop_queue' (pq)
'start_queue' (sq).
END_WARN
    }

    $manager->start_listener;
}

sub run_stop {
    my ( $package, $script_call, $manager ) = @_;

    if ( my $queue_selection = $script_call->value_of('queue_selection') ) {
        warn <<"END_WARN";
! Attention !
Action 'stop' stops whole listener, so queue selection ($E{
    join ', ', map {"-$_"} keys %{$queue_selection}
}).
For particular queue manipulation use actions 'stop_queue' (pq)
'start_queue' (sq).
END_WARN
    }

    $manager->stop_listener;
}

sub run_restart {
    my ( $package, $script_call, $manager ) = @_;

    $manager->restart_listener;
}

sub run_wakeup {
    my ( $package, $script_call, $manager ) = @_;

    $manager->wakeup_listener;
}

# move messages from exception queue
sub run_eqh {
    my ( $package, $script_call, $manager ) = @_;

    my $target_queue      = $script_call->value_of('target_queue');
    my $message_selection = $script_call->value_of('message_selection');
    die "No message selection, to select all messages use --all_messages (-ma)\n"
        if !$message_selection;
    for my $queue ( $package->select_queues($manager,  $script_call, 1 ) ) {
        $manager->move_messages($queue, $message_selection, $target_queue );
    }
}

sub run_move {
    my ( $package, $script_call, $manager ) = @_;

    my $target_queue      = $script_call->value_of('target_queue');
    my $message_selection = $script_call->value_of('message_selection');
    die "No message selection, to select all messages use --all_messages (-ma)\n"
        if !$message_selection;

    for my $queue ( $package->select_queues($manager,  $script_call, 0 ) ) {
        $manager->move_messages($queue, $message_selection, $target_queue );
    }
}

sub run_remove {
    my ( $package, $script_call, $manager ) = @_;

    my $message_selection = $script_call->value_of('message_selection');
    die
        "No message selection, to select all messages use --all_messages (-ma)\n"
        if !$message_selection;
    for my $queue (
        $package->select_queues( $manager, $script_call ) )
    {
        $manager->remove_messages($queue, $message_selection);
    }
}

# prints the logfile
sub run_logfile {
    my ( $this, $script_call, $manager ) = @_;

    for my $controller ( $this->select_queues($manager, $script_call) ) {
        print $controller->logpath, "\n";
    }
}

sub run_start_queue {
    my ( $this, $script_call, $manager ) = @_;

    $manager->start_queues( $this->select_queues($manager, $script_call) );
}

sub run_stop_queue {
    my ( $this, $script_call, $manager ) = @_;

    $manager->stop_queues( $this->select_queues($manager, $script_call) );
}

sub run_start_all_queues {
    my ( $this, $script_call, $manager ) = @_;

    $manager->start_all_queues;
}

sub run_set_auto_start {
    my ( $this, $script_call, $manager ) = @_;

    $manager->txn_do(
        sub {
            for my $queue (
                $this->select_queues( $manager, $script_call, 0 ) )
            {
                $manager->set_auto_start($queue, 1);
            }
        }
    );
}

sub run_reset_auto_start {
    my ( $this, $script_call, $manager ) = @_;

    $manager->txn_do(
        sub {
            for my $queue (
                $this->select_queues( $manager, $script_call, 0 ) )
            {
                $manager->set_auto_start($queue, 0);
            }
        }
    );
}

sub run_start_queue_for_enqueue {
    my ( $this, $script_call, $manager ) = @_;

    for my $queue ( $this->select_queues( $manager, $script_call ) )
    {
        next
            if $queue->{is_exception_queue}
            || $queue->{enqueue_enabled};


        $manager->start_queue($queue, 1, 0 );
        warn "Queue $E{ $queue->{name} } started for enqueue.\n";
    }
}

sub run_stop_queue_for_enqueue {
    my ( $this, $script_call, $manager ) = @_;

    for my $queue ( $this->select_queues( $manager, $script_call ) )
    {
        next
            if $queue->{is_exception_queue}
            || !$queue->{enqueue_enabled};

        $manager->stop_queue($queue, 1, 0 );
        warn "Queue $E{ $queue->{name} } stopped for enqueue\n";
    }
}

sub run_clone_queue {
    my ( $this, $script_call, $manager ) = @_;

    my @queues = $this->select_queues( $manager, $script_call, 0 );
    @queues == 1 or die "Only one queue can be specified for clone_queue\n ";
    my ($queue) = @queues;

    my $new_queue = $script_call->value_of('new_queue');
    $manager->txn_do(
        sub {
            $manager->clone_queue($queue, $new_queue, $script_call->value_of('new_queue_params') );
        }
    );

    warn "Queue $new_queue created\n";
}

sub run_drop_queue {
    my ( $this, $script_call, $manager ) = @_;

    my @queues = $this->select_queues( $manager, $script_call, 0 );
    @queues == 1
        or die "Only one queue can be specified for drop_queue\n ";
    my ($queue) = @queues;

    $manager->do_drop_queue( $queue);


    warn "Queue $queue->{name} dropped\n";
}

sub select_queues {
    my ( $this, $manager, $script_call, $exception_only ) = @_;

    if ( !defined($exception_only)
        && $script_call->value_of('exception_queue') )
    {
        $exception_only = 1;
    }

    my @queues = grep {
        !defined($exception_only)
            || ( $exception_only xor !$_->{is_exception_queue} );
    } $manager->list_queues;

    # the name directly set, it can be RE
    if ( my $queue = $script_call->value_of('queue') ) {
        my @selected = grep { $_->{name} =~ /^$queue$/i } @queues
        or die "No $E{ $exception_only ? 'exception ': ''}queue '$queue' exists.\n";
        return @selected;
    }
    return @queues if $script_call->value_of('all_queues');

    return $queues[0];
}

# returns the controllers selected according to command line options

sub run_testrun {
    my ( $this, $script_call, $manager ) = @_;

    my @controllers = $this->select_queues( $manager, $script_call, 0 );
    die "Action 'testrun' should be run for one queue only\n "
        if @controllers > 1;

    $controllers[0]->testrun( grep {$_} $script_call->value_of('daemon_control') );
}

1;

__END__

=head1 NAME

Rambutan::AQ::CLI - Command line interface for Rambutan::AQ

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 AUTHOR

=cut

vim: expandtab:shiftwidth=4:tabstop=4:softtabstop=0:textwidth=78:
