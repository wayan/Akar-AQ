create or replace package rambutan.pkg_aq as
-- $Id: pkg_aq.sql,v 1.2 2008/03/28 13:13:26 cvs Exp $
-- Queue selection 

procedure set_daemon_queue_for(
    -- queue table to set the queue for
    in_queue_table in rambutan.aq_listener.queue_table%TYPE,
    in_queue       in rambutan.aq_daemon.queue%TYPE 
);

procedure set_queue_for(
    -- queue table to set the queue for
    in_queue_table in rambutan.aq_listener.queue_table%TYPE,
    in_queue       in rambutan.aq_daemon.queue%TYPE 
);

function select_queue_from(
    -- queue table to select the queue from
    in_queue_table in rambutan.aq_listener.queue_table%TYPE,
    -- suggested name of the queue - uses when exists
    in_queue in rambutan.aq_daemon.queue%TYPE default null)
return rambutan.aq_daemon.queue%TYPE;

function select_daemon_queue_from(
    -- queue table to select the queue from
    in_queue_table in rambutan.aq_listener.queue_table%TYPE,
    -- suggested name of the queue - uses when exists
    in_queue in rambutan.aq_daemon.queue%TYPE default null)
return rambutan.aq_daemon.queue%TYPE;

end pkg_aq;
/
show errors

grant execute on rambutan.pkg_aq to public;



