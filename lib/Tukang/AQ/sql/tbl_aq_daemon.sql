--$Id: tbl_aq_daemon.sql,v 1.4 2008/07/28 12:38:39 danielr Exp $
create table rambutan.aq_daemon (
    id number(10) not null,
    queue varchar2(256) not null,
    private_dc_queue varchar2(64) constraint u_aq_daemon_dcq unique,
    daemon_num number(3) default 0 not null,
    usessionid varchar2(64),
    pid number(5) not null,
    started date default sysdate not null,
    being_stopped date,
    constraint pk_aq_daemon primary key (id));

create sequence rambutan.aq_daemon_seq start with 1 increment by 1;
create unique index rambutan.aq_daemon_idx on rambutan.aq_daemon (queue, daemon_num);

grant all on rambutan.aq_daemon to public;
grant all on rambutan.aq_daemon_seq to public;

comment on table rambutan.aq_daemon is 'Daemons dequeueing from AQS2 queues';
comment on column rambutan.aq_daemon.usessionid is 'Daemon session id';
comment on column rambutan.aq_daemon.pid is 'Daemon PID';


