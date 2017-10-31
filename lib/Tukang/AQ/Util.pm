package Tukang::AQ::Util;

use common::sense;

use Exporter 'import';
use Carp qw(carp croak);
use Types::Standard qw(Str Dict Optional Any slurpy CodeRef HasMethods Bool HashRef slurpy Int);
use Type::Params ();

# methods exported on demand (preferred)
our @EXPORT_OK = qw(dbh_err wait_until);

# 2009-07-02 danielr
# since I have sudden problem with $this->db_Main->err returning undef (caused
# by AutoCommit=1?) I add the detection of error
sub dbh_err {
    my ( $dbh, $error ) = @_;

    my $err = $dbh->err;
    return $err if $err;

    # it is a bit evil and works for oracle only
    my ($code) = $error =~ /\bORA-(\d+):/
        or return;
    return $code;
}

sub wait_until {
    state $params_check = Type::Params::compile(
        slurpy Dict[
            first_timeout => Optional[ Int ],
            max_timeout   => Optional[ Int ],
            condition      => CodeRef
        ]
    );
    my ($params) = $params_check->(@_);

    # initial waiting timeout (in miliseconds)
    my $sleep_timeout = $params->{first_timeout} // 100;
    my $max_timeout   = $params->{max_timeout}   // 15000;
    my $condition     = $params->{condition};

    my $last_sleep_timeout;
    my $started = time;
    while ( 1 ){
        return 0 if 1000 * ( time - $started ) > $max_timeout;    # timeout

        # incremental sleeping
        Time::HiRes::usleep($sleep_timeout);

        return 1 if $condition->();

        # fibonacci number - I heard that they are good for repetitive tries
        my $new_sleep_timeout
            = $sleep_timeout + ( $last_sleep_timeout || $sleep_timeout );
        $last_sleep_timeout = $sleep_timeout;
        $sleep_timeout      = $new_sleep_timeout;
    }

}



1;

# vim: expandtab:shiftwidth=4:tabstop=4:softtabstop=0:textwidth=78:
