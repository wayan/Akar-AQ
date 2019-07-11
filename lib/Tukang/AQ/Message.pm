package Tukang::AQ::Message;

use common::sense;

use base qw(Class::Accessor);

use Carp qw(carp croak);
use Akar::DBI::Statement qw(sql_param sql sql_param_inout sql_and sql_join);
use Interpolation 'E' => 'eval', 'sqlp' => sub { return sql_param(@_) };

sub set_payload_fields {
    my $class = shift;

    my @names;
    my %properties;
    for my $name_or_prop (@_) {
        if ( ref($name_or_prop) ) {

            # options
            $properties{ $names[-1] } = $name_or_prop;
        }
        else {

            # name
            push @names, $name_or_prop;
            $properties{$name_or_prop} = {};
        }
    }

    no strict 'refs';
    *{ $class . '::' . 'payload_fields' } = sub { return @names };
    *{ $class . '::' . 'payload_field_properties' }
        = sub { return \%properties };

    $class->mk_accessors(@names);
}

sub payload_fields {
    my ($class) = grep { ref($_) || $_ } shift();
    die <<"END_EXCLAMATION"
No payload_fields has been defined for class $class.
Use $class->set_payload_fields(\@fields);
END_EXCLAMATION
}

sub list_headers { 'Data' }

sub handle {
    my ( $this, $message_properties, $msgid ) = @_;

    $this->process;
}

sub handle_error {}

sub process { die "No process defined"; }

# returns 1 or 2 PL/SQL expression for the queue selection
# if 1 it is the expression for queue selection
# if 2 the first is the declaration of function
#   the second is the expression where the function may be used
sub target_queue_sql {
    my ( $this, $manager, $source_queue, $options ) = @_;

	# attention - the parameters are not settled yet

    # first queue in queue table
    my ($first_queue) =
      grep { !$_->{is_exception_queue} && $_->{enqueue_enabled} }
      $manager->list_queues
		or die "No available target queue";

    return sql_param( $first_queue->{qname} );
}

# returns where condition for
sub selection_sql {
    my ( $this, $options ) = @_;

    my @selection;
    if ( my $msgid = $options->{'msgid'} ) {

        # selected by message id
        push @selection, sql("msgid = $sqlp{$msgid}");
    }
	# 2013-03-04 multiconsumer queue
    if ( my $consumer_name = $options->{'consumer_name'} ) {
        push @selection, sql("consumer_name = $sqlp{$consumer_name}");
    }
    if ( $options->{'active_messages'} ) {
        push @selection, sql("state = 0");
    }

    # message key
    if ( my $message_key = $options->{'message_key'} ) {

        # message key is a hash reference
        while ( my ( $key, $value ) = each %{$message_key} ) {
            push @selection, sql("qt.user_data.$key = $sqlp{$value}");
        }
    }

    # any_selection
    if ( my $any_selection = $options->{'any_selection'} ) {
        push @selection, $any_selection;
    }

    return sql_and(@selection, '1=1');
}

sub list_payload {
    my ( $this, $extended_view ) = @_;

    return join(
        ', ',
        map {
            my $value = $this->$_;
            $_ . '=' . ( defined($value) ? $value : 'null' );
        } $this->payload_fields
    );
}

# code to be inserted between dequeue and enqueue
sub on_move_psql {
    return;
}

1;

__END__

=head1 NAME

Tukang::AQ::Message - SUPPLY SHORT DESCRIPTION OF THE PACKAGE

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 AUTHOR

=cut

vim: shiftwidth=4:tabstop=4:softtabstop=0:
