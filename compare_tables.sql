CREATE OR REPLACE FUNCTION comparelargeObj (v_stmt_1 in varchar, v_data_type in varchar, colname in varchar,v_stmt_2 in varchar) RETURN INTEGER IS
	pragma autonomous_transaction;
	v_ret_code integer := 0 ;
	stmt varchar2(1024) ;
	stmt1 varchar2(1024) ;
	blob_v_1 blob ;
	blob_v_2 blob ;
	clob_v_1 clob ;
	clob_v_2 clob ;
	nclob_v_1 NCLOB;
	nclob_v_2 nclob; 
	raw_v_1 raw(1024);
	raw_v_2 raw(1024);	
	long_v_1 clob ;
	long_v_2 clob;
	
	c1 sys_refcursor;
	c2 sys_refcursor;
BEGIN 
	--dbms_output.put_line(v_stmt_1);
	--dbms_output.put_line(v_stmt_2);
	open c1 for (v_stmt_1);
	open c2 for (v_stmt_2);
	LOOP
		case v_data_type 
		
			when 'BLOB' THEN 
				FETCH c1 INTO blob_v_1;
				FETCH c2 INTO blob_v_2;
			when 'CLOB' then 
				FETCH c1 INTO clob_v_1;
				FETCH c2 INTO clob_v_2;
			when 'NCLOB' THEN  
				FETCH c1 INTO nclob_v_1;
				FETCH c2 INTO nclob_v_1;
			when 'XMLTYPE' THEN 
				FETCH c1 INTO clob_v_1;
				FETCH c2 INTO clob_v_2;
			when 'LONG' THEN 
				FETCH c1 INTO long_v_1;
				FETCH c2 INTO long_v_2;
			when 'RAW' THEN 
				FETCH c1 INTO raw_v_1 ;
				FETCH c2 INTO raw_v_1 ;
			else 
				exit;
		end case;
		 IF (c1%FOUND) AND (c2%FOUND) THEN
			case v_data_type 
			when 'BLOB' then 
				if dbms_lob.compare( blob_v_1,blob_v_2) is null or dbms_lob.compare( blob_v_1,blob_v_2) != 0  THEN 
					v_ret_code := 1;
					return v_ret_code;	
				end if;
			when 'CLOB' then 
				if dbms_lob.compare( clob_v_1,clob_v_2) is null or dbms_lob.compare( clob_v_1,clob_v_2) != 0  THEN 
					v_ret_code := 1;
					return v_ret_code;	
				end if;
			when 'XMLTYPE' then 
				if dbms_lob.compare( clob_v_1,clob_v_2) is null or dbms_lob.compare( clob_v_1,clob_v_2) != 0  THEN 
					v_ret_code := 1;
					return v_ret_code;	
				end if;	
			when 'NCLOB' then 
				if dbms_lob.compare( nclob_v_1,nclob_v_2) is null or dbms_lob.compare( nclob_v_1,nclob_v_2) != 0  THEN 
					v_ret_code := 1;
					return v_ret_code;	
				end if;	
			when 'RAW' then 
				if dbms_lob.compare( raw_v_1,raw_v_2) is null or dbms_lob.compare( raw_v_1,raw_v_2) != 0  THEN 
					v_ret_code := 1;
					return v_ret_code;	
				end if;	
			when 'LONG' then 			
				if dbms_lob.compare(long_v_1 ,long_v_2 ) is null or dbms_lob.compare(long_v_1,long_v_2) != 0  THEN 
					v_ret_code := 1;
					return v_ret_code;
				end if;
			end case;
		 ELSE
			EXIT;			
		END IF;		
	END LOOP;
	CLOSE c1;
	CLOSE c2;
	return v_ret_code;
END;
/
CREATE OR REPLACE FUNCTION compareTables (v_table_1 in varchar, v_table_2 in varchar) RETURN INTEGER IS
	pragma autonomous_transaction;
	v_return_code INTEGER;	
	v_no_of_records_tab_1 INTEGER ;
	v_no_of_records_tab_2 INTEGER ;
	v_no_of_rec_after_cmp INTEGER ;
	misc_cnt INTEGER;
	stmt1 varchar2(4048);
	stmt2 varchar2(4048);
	stmt3 varchar2(4048);
	cmp_stmt varchar2(4048);
	blob_v_1 blob ;
	blob_v_2 blob ;
	clob_v_1 clob ;	
	clob_v_2 clob;
	v_ret_code integer ;
	cursor c1 is select COLUMN_NAME from cols where table_name = v_table_1 and data_type NOT IN ('BLOB','CLOB','XMLTYPE','LONG','RAW','NCLOB');	
	cursor c2 is select data_type , COLUMN_NAME, case data_type when 'XMLTYPE' then 'select to_clob(' || COLUMN_NAME || ') from ' ELSE 'select ' || COLUMN_NAME || ' from ' end as sqlstmt from cols where table_name = v_table_1  and data_type IN ('BLOB','CLOB','XMLTYPE','NCLOB','RAW','LONG');
	
BEGIN
	
	v_return_code := 0;
	
	-- First Check: Checking if the count of both the tables are same --
	
	stmt1 := 'select count(1) from ' || v_table_1;
	execute immediate stmt1 into v_no_of_records_tab_1;
	stmt1 := 'select count(1) from ' || v_table_2;	
	execute immediate stmt1 into v_no_of_records_tab_2;	
	
	if v_no_of_records_tab_2 != v_no_of_records_tab_1 then 
		v_return_code := 1;
		RETURN v_return_code;
	end if;
	
	-- First check passes -- 
	-- Second Check: Checking for all the non 'BLOB','CLOB','XMLTYPE','LONG','RAW','NCLOB'  data type --  
	-- Assuming that the table structure is same including the metadata --	
	for column_list in c1 loop
		--dbms_output.put_line(column_list.column_name || ' ' || column_list.data_type);
		stmt1 := 'select count(1) from ( select '|| column_list.column_name || ' from ' || v_table_1 || ' minus select ' || column_list.column_name || ' from ' || v_table_2 || ') ';
		execute immediate stmt1 into v_no_of_records_tab_1 ;
		if v_no_of_records_tab_1 != 0 then 
			v_return_code := 1;
			return v_return_code;
		end if;		
	end loop; 	
	
	-- Second check passes -- 
	-- Third Check: Checking for all the 'BLOB','CLOB','XMLTYPE','LONG','RAW','NCLOB'  data type --  
	for rec in c2 loop
		--stmt2 := rec.sqlstmt || v_table_1 || ' where ' || rec.COLUMN_NAME || ' IS NOT NULL ';
		stmt2 := 'select '|| rec.COLUMN_NAME || ' from ' || v_table_1 || ' where ' || rec.COLUMN_NAME || ' IS NOT NULL ';
		stmt3 := 'select '|| rec.COLUMN_NAME || ' from ' || v_table_2 || ' where ' || rec.COLUMN_NAME || ' IS NOT NULL ';
		--stmt3 := rec.sqlstmt || v_table_2 || ' where ' || rec.COLUMN_NAME || ' IS NOT NULL ';
		if rec.data_type = 'XMLTYPE' THEN 			
			
			stmt2 := 'select to_clob( '|| rec.COLUMN_NAME || ') from ' || v_table_1 || ' where ' || rec.COLUMN_NAME || ' IS NOT NULL ';
			stmt3 := 'select to_clob( '|| rec.COLUMN_NAME || ') from ' || v_table_2 || ' where ' || rec.COLUMN_NAME || ' IS NOT NULL ';
		
		end if;
		if rec.data_type = 'LONG' THEN	
			select count(*) into misc_cnt from tab where lower(tname) = 'temp_long_1';
			if misc_cnt =1 then 
				stmt2 := 'drop table temp_long_1';
				execute immediate stmt2;
			end if ;
			--stmt2 := 'delete from temp_long_1';
			stmt2 := 'delete from temp_long_1';
			execute immediate stmt2;
			stmt2 := 'delete from temp_long_2';
			execute immediate stmt2;
			stmt2 := 'insert into temp_long_1 select to_lob('|| rec.COLUMN_NAME || ') from ' || v_table_1 ;
			stmt3 := 'insert into temp_long_2 select to_lob('|| rec.COLUMN_NAME || ') from ' || v_table_2 ;
			execute immediate stmt2;
			execute immediate stmt3;
			commit;
			stmt2 := 'select '|| rec.COLUMN_NAME || ' from temp_long_1' || ' where ' || rec.COLUMN_NAME || ' IS NOT NULL ';
			stmt3 := 'select '|| rec.COLUMN_NAME || ' from temp_long_2' || ' where ' || rec.COLUMN_NAME || ' IS NOT NULL ';
			
		end if;

		select comparelargeObj(stmt2,rec.data_type,rec.COLUMN_NAME,stmt3) into v_ret_code from dual;
		v_return_code := v_ret_code;
	end loop ;
	
	RETURN v_return_code;
END;
/
