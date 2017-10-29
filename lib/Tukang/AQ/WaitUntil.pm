package Tukang::AQ::WaitUntil;

use common::sense;

use base qw(Class::Accessor::Fast);

use Carp qw(carp croak);
use Time::HiRes;

__PACKAGE__->mk_accessors(qw(first_timeout max_timeout waited conditions));

use overload '&{}' => sub {
    my $this = shift;
    return sub { $this->check }
    },
    'fallback' => 1;

sub new {
    my ( $class, $fields ) = @_;

    $fields ||= {};
    return $class->SUPER::new(
        {

            # initial waiting timeout (in miliseconds)
            'first_timeout' => 100,

            # maximum waiting period (in miliseconds)
            'max_timeout' => 15000,
            %{$fields},

            # check is list of the hashes
            'conditions' => [],
        }
    );
}

sub DESTROY {
    my ($this) = @_;
    $this->wait_until if !$this->waited;
}

sub cancel {
    my ($this) = @_;

    # pretend it already waited
    $this->waited(1);
}

# waits (periodically) until condition is true
# condition may be list of subs
sub wait_until {
    my ($this) = @_;

    $this->waited(1);

    # initial waiting timeout (in miliseconds)
    my $sleep_timeout = $this->first_timeout;

    # maximum timeout
    my $max_timeout = $this->max_timeout;
    my $last_sleep_timeout;

    my $started = time;
    while ( $this->check ) {
        last if 1000 * ( time - $started ) > $max_timeout;    # timeout

        # incremental sleeping
        Time::HiRes::usleep($sleep_timeout);

        # fibonacci number - I heard that they are good for repetitive tries
        my $new_sleep_timeout
            = $sleep_timeout + ( $last_sleep_timeout || $sleep_timeout );
        $last_sleep_timeout = $sleep_timeout;
        $sleep_timeout      = $new_sleep_timeout;
    }

    my @conditions = @{ $this->conditions }
    or return 1;

    # runs the on_failure checks
    for my $condition ( @{ $this->conditions } ) {
        my $on_failure = $condition->{'on_failure'}
            or next;
        $on_failure->();
    }
    return 0;
}

# returns the checks which weren't successfull
sub check {
    my ($this) = @_;

    my @conditions = grep {
        my ( $check, $on_success ) = @{$_}{qw(check on_success)};

        my $ok = $check->();
        if ( $ok && $on_success ) {
            $on_success->();
        }
        !$ok;
    } @{ $this->conditions };
    $this->conditions(\@conditions);
    return @conditions;
}

sub add {
    my ( $this, %params ) = @_;

    push @{$this->conditions}, \%params;
}

1;

__END__

=head1 NAME

Tukang::AQ::WaitUntil - periodically checks the set of conditions until they are all true or timeout occured

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 AUTHOR

=cut

vim: expandtab:shiftwidth=4:tabstop=4:softtabstop=0:textwidth=78:
