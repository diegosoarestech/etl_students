
create database producao;

use producao;

drop table if exists fact_events;

create table fact_events
(
  session_int_id bigint
, session_start_at datetime
, student_cli varchar(50)
, student_id varchar(200)
);

drop table if exists dim_sessions;

create table dim_sessions
(
  session_int_id bigint
, session_start_at datetime
, student_cli varchar(50)
, student_id varchar(200)
);

drop table if exists dim_subscriptions;

create table dim_subscriptions
(
  subscription_int_id bigint
, plan_type varchar(50)
, payment_dt datetime
, student_id varchar(200)
);

drop table if exists dim_student_subject;

create table dim_student_subject
(
  student_subject_int_id bigint
, subject_id int
, subject_nm varchar(200)
, subject_follow_dt datetime
, student_id varchar(200)
);

drop table if exists dim_courses;

create table dim_courses
(
  course_int_id bigint
, course_id bigint
, course_nm varchar(200)
);

drop table if exists dim_universities;

create table dim_universities
(
  university_int_id bigint
, university_id bigint
, university_nm varchar(200)
);


drop table if exists dim_students;

create table dim_students
(
  student_int_id bigint
, student_id varchar(200)
, registered_dt datetime
, signup_src varchar(200)
, student_cli varchar(200)
, student_state varchar(200)
, student_city varchar(200)
, course_id bigint
, university_id bigint
);

