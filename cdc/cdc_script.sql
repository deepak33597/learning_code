

--->Change Data Capture -->

-- By Db Queryes
To change Data in source system -- Use case when No Timestamp present to identify (date) which data is inserted 
and which is updated -->


Target system send --> by ETL Tool like Talend or Pentaho -- for incrememtal load
---------------------------------------------------------------------------------------------------------------->


Drop table if exists empltable;
Drop table if exists empltable_cdc_log_capture;
Drop trigger  if exists triggr_empltable_cdc_log_capture;

Create Table empltable
(
emp_id int identity(1,1) primary key NOT NULL,
emp_name varchar(100),
emp_salary int
);


create table empltable_cdc_log_capture
(
id int identity(1,1) primary key NOT NULL,
emp_id int,
emp_name varchar(100),
old_emp_salary int,
new_emp_salary int,
event_date datetime,
opration_status varchar(10)

)

Drop trigger  if exists triggr_empltable_cdc_log_capture;
create  trigger triggr_empltable_cdc_log_capture
on empltable
AFTER INSERT,UPDATE,DELETE
As
Begin 
DECLARE @operation_n_table varchar(10)
SET @operation_n_table=case when EXISTS (select * from deleted) and EXISTS (select * from inserted) then 'UPDATE' 
                            when EXISTS (select * from inserted) THEN 'INSERT'
                            when EXISTS (select * from deleted) THEN 'DELETE'							
	                        else NULL END	
IF @operation_n_table ='INSERT' 
INSERT INTO PartitioningDB.dbo.empltable_cdc_log_capture
     ( emp_id, emp_name, old_emp_salary, new_emp_salary,event_date,opration_status)
      Select  
      i.emp_id, i.emp_name, null, i.emp_salary as new_emp_salary, GETDATE(),@operation_n_table
      new_salary_credited_date
      from inserted i
IF @operation_n_table ='UPDATE'
INSERT INTO PartitioningDB.dbo.empltable_cdc_log_capture
     ( emp_id, emp_name, old_emp_salary, new_emp_salary,event_date,opration_status)
      Select  
      i.emp_id, i.emp_name, d.emp_salary as old_emp_salary, i.emp_salary as new_emp_salary, GETDATE(),@operation_n_table
      new_salary_credited_date
      FROM deleted d, inserted i

IF @operation_n_table ='DELETE'
INSERT INTO PartitioningDB.dbo.empltable_cdc_log_capture
     ( emp_id, emp_name, old_emp_salary, new_emp_salary,event_date,opration_status)
      Select  
      d.emp_id, d.emp_name, null as old_emp_salary, d.emp_salary as new_emp_salary, GETDATE(),@operation_n_table
      new_salary_credited_date
      FROM deleted d
END


insert into empltable
select 'sam1' as emp_name , 2000 emp_salary
union all
select 'sam' as emp_name , 2000 emp_salary

update  empltable SET  emp_salary=3000 where emp_id=1

select * from empltable;

select * from empltable_cdc_log_capture;

