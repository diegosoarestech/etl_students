create database stage;

use stage;

drop table if exists stg_students;

create table stg_students (
	student_int_id      bigint not null auto_increment,
	student_id          varchar(200),
	registered_dt       timestamp,
	signup_src 			varchar(200),
	student_cli			varchar(200),
	student_state		varchar(200),
	student_city		varchar(200),
	course_id			bigint,
	university_id  		bigint,
    primary key(student_int_id)
);

create index idx_stg_students_student_id on stg_students (student_id);
create index idx_stg_students_course_id  on stg_students (course_id);
create index idx_stg_students_university_id on stg_students (university_id);

drop table if exists stg_universities;

create table stg_universities (
	university_int_id       bigint not null auto_increment,
	university_id			bigint,
	university_nm   		varchar(200),
    primary key(university_int_id)
);

create index idx_stg_universities_university_id on stg_universities (university_id);

drop table if exists stg_courses;

create table stg_courses (
	course_int_id       bigint not null auto_increment,
	course_id	   		bigint,
	course_nm      		varchar(200),
    primary key(course_int_id)
);

create index idx_stg_courses_course_id on stg_courses (course_id);

drop table if exists stg_student_subject;

create table stg_student_subject (
	student_subject_int_id  bigint not null auto_increment,
    subject_id				int,
	subject_nm				varchar(200),
	subject_follow_dt   	timestamp,
    student_id				varchar(200),
    primary key(student_subject_int_id)
);

create index idx_stg_student_subject on stg_student_subject (student_id);

drop table if exists stg_subscriptions;

create table stg_subscriptions (
	subscription_int_id     bigint not null auto_increment,
	plan_type				varchar(50),
	payment_dt   			timestamp,
    student_id 				varchar(200),
    primary key(subscription_int_id)
);

create index idx_stg_subscriptions on stg_subscriptions (student_id);

drop table if exists stg_sessions;

create table stg_sessions (
	session_int_id     bigint not null auto_increment,
	session_start_at   timestamp,
	student_cli		   varchar(50),
    student_id 		   varchar(200),
    primary key(session_int_id)
);


create index idx_stg_sessions on stg_sessions (student_id);


drop table if exists stg_events;

create table stg_events (
event_int_id					bigint not null auto_increment,
event_id 						text ,
event_at   						timestamp ,
event_at_dt       			    date ,
last_accessed_url     			text ,
page_category     			    text ,
page_name     			        text ,
browser 						text ,
carrier						    text ,
clv_total 						bigint ,
user_frequency 					text ,
device_new 						boolean ,
event_language  				text ,
marketing						text ,
user_type						text ,
platform						text ,
permanence						long,
student_id 						varchar(200),
primary key(event_int_id)
);

create index idx_stg_events on stg_events (student_id);


