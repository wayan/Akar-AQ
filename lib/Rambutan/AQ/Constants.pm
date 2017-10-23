package Rambutan::AQ::Constants;

use strict;
use warnings;


use Carp qw(carp croak);
use base qw(Exporter);

my %constants = (

#------------------------------------------------------------------------------
# PL/SQL exception codes
#
    'LISTEN_TIMEOUT_ERRNO'  => 25254,
    'DEQUEUE_TIMEOUT_ERRNO' => 25228,

    # ORA-25263: no message in queue
    # when dequeue by msgid
    'NO_MESSAGE_IN_QUEUE_ERRNO' => 25263,

#------------------------------------------------------------------------------

    # queue types
    'NORMAL_QUEUE'    => 'NORMAL_QUEUE',
    'EXCEPTION_QUEUE' => 'EXCEPTION_QUEUE',
);

use constant;
constant->import(\%constants);
our @EXPORT_OK = keys %constants;

1;

__END__

=head1 NAME

Rambutan::AQ::Constants - SUPPLY SHORT DESCRIPTION OF THE PACKAGE

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 AUTHOR

=cut

vim: expandtab:shiftwidth=4:tabstop=4:softtabstop=0:textwidth=78:
