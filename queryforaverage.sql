use bhavcopydata;
select * from equity;
select count(*) from equity;
select sc_name,avg(open) as open_price,avg(high) as high_price,avg(low) as low_price,avg(close) as close_price from equity group by sc_name;
