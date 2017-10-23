--$Id: tbl_aq_listener.sql,v 1.3 2008/07/24 09:37:26 danielr Exp $
create table rambutan.aq_listener (
    id number(10) not null,
    queue_table varchar2(256) not null,
    -- daemon control queue
    private_dc_queue varchar2(64) constraint u_aq_listener_dcq unique,
    single_consumer_dc_queue number(1) default 0 not null,
    usessionid varchar2(64),
    hostname varchar2(128) not null,
    pid number(5) not null,
    started date default sysdate not null,
    being_stopped date,
    constraint pk_aq_listener primary key (id));

create unique index rambutan.aq_listener on rambutan.aq_listener (queue_table);

grant all on rambutan.aq_listener to public;
grant all on rambutan.aq_listener to public;

comment on table rambutan.aq_listener is 'Listener listening on all queues of advanced queue table';
comment on column rambutan.aq_listener.usessionid is 'Daemon session id';
comment on column rambutan.aq_listener.pid is 'Daemon PID';


