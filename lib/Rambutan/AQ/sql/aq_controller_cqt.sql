-- $Id: aq_controller_cqt.sql,v 1.2 2008/03/28 13:13:26 cvs Exp $
-- control queue
create type rambutan.aq_controller_dc_t as object (
    -- akce, která se má provést
    action varchar2(12),
    -- fronta (démon, který ji poslal)
    sender varchar2(48)
    );
/

grant execute on rambutan.aq_controller_dc_t to public;

begin
sys.dbms_aqadm.create_queue_table(
    queue_table => 'rambutan.aq_controller_dc_qt',
    queue_payload_type => 'RAMBUTAN.AQ_CONTROLLER_DC_T',
    -- compatible => '8.0',
    multiple_consumers => true);
end;
/

-- the only control queue
begin
sys.dbms_aqadm.create_queue(
    queue_name  => 'aq_controller_dc_q',
    queue_table => 'rambutan.aq_controller_dc_qt',
    -- opakování nemá smysl
    max_retries => 0);
end;
/

begin
sys.dbms_aqadm.start_queue(queue_name => 'rambutan.aq_controller_dc_q');
end;
/


-- vim: expandtab:shiftwidth=4:tabstop=4:softtabstop=0:textwidth=78: 

