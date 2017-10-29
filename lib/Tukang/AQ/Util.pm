package Tukang::AQ::Util;

use common::sense;

use Exporter 'import';
use Carp qw(carp croak);

# methods exported on demand (preferred)
our @EXPORT_OK = qw(dbh_err);

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

1;

# vim: expandtab:shiftwidth=4:tabstop=4:softtabstop=0:textwidth=78:
