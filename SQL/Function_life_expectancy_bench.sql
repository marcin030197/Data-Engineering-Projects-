CREATE OR REPLACE FUNCTION project.life_expectancy_bench(life_expectancy double precision)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare 
	status_out integer;
begin
	if (life_expectancy>64)   then 
		status_out=1;
		
	else
		status_out=0;
	end if;
	
	return status_out;
end;
$function$
;
