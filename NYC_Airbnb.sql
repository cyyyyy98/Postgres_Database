create table if not exists bookings(
 booking_id int primary key,
 listing_name varchar,
 host_id int,
 host_name varchar(50),
 neighbourhood_group varchar(30),
 neighbourhood varchar(30),
 latitude decimal(11,8),
 longitude decimal(11,8),
 room_type varchar(30),
 price int,
 minimun_nights int,
 num_of_reviews int,
 last_review date,
 reviews_per_month decimal(4,2),
 calculated_host_listings_count int,
 avilability_365 int
);

select * from bookings;

select
	booking_id,
	listing_name,
	neighbourhood_group,
	avg(price) over()
from bookings;

select
	booking_id,
	listing_name,
	neighbourhood_group,
	avg(price) over(),
	min(price) over(),
	max(price) over()
from bookings;

-- Difference from average price with OVER
select
	booking_id,
	listing_name,
	neighbourhood_group,
	price,
	round(avg(price) over (),2),
	round((price - avg(price) over()),2) as diff_from_avg
from bookings;

-- Percentage of average price with OVER
select
	booking_id,
	listing_name,
	neighbourhood_group,
	price,
	round(avg(price) over (),2) as avg_price,
	round((price / avg(price) over() * 100),2) as percent_of_avg_price
from bookings;

-- Percentage difference from average price
select
	booking_id,
	listing_name,
	neighbourhood_group,
	price,
	round(avg(price) over (),2) as avg_price,
	round((price / avg(price) over() - 1) * 100,2) as percent_diff_from_avg_price
from bookings;

-- Partition by neighbourhood group
select
	booking_id,
	listing_name,
	neighbourhood_group,
	neighbourhood,
	price,
	avg(price) over(partition by neighbourhood_group) as avg_price_by_neigh_group
from bookings;

select distinct neighbourhood_group from bookings;

-- Partition by neighbourhood group and neighbourhood
select
	booking_id,
	listing_name,
	neighbourhood_group,
	neighbourhood,
	price,
	avg(price) over(partition by neighbourhood_group) as avg_price_by_neigh_group,
	avg(price) over (partition by neighbourhood_group, neighbourhood) as avg_price_by_group_and_neigh
from bookings;

-- Neighbourhood and neighbourhood group and neighbourhood delta
select
	booking_id,
	listing_name,
	neighbourhood_group,
	neighbourhood,
	price,
	avg(price) over(partition by neighbourhood_group) as avg_price_by_neigh_group,
	avg(price) over(partition by neighbourhood_group, neighbourhood) as avg_price_by_group_and_neigh,
	round(price - avg(price) over(partition by neighbourhood_group),2) as neigh_group_delta,
	round(price - avg(price) over(partition by neighbourhood_group, neighbourhood),2) as group_and_neigh_delta
from bookings;

-- overall price rank
select
	booking_id,
	listing_name,
	neighbourhood_group,
	neighbourhood,
	price,
	row_number() over(order by price desc) as overall_price_rank
from bookings;

-- Neighbourhood price rank
select
	booking_id,
	listing_name,
	neighbourhood_group,
	neighbourhood,
	price,
	row_number() over(order by price desc) as overall_price_rank,
	row_number() over(partition by neighbourhood_group order by price desc) as neigh_group_price_rank
from bookings;

-- Top 3
select
	booking_id,
	listing_name,
	neighbourhood_group,
	neighbourhood,
	price,
	row_number() over(order by price desc) as overall_price_rank,
	row_number() over(partition by neighbourhood_group order by price desc) as neigh_group_price_rank,
	case
		when row_number() over(partition by neighbourhood_group order by price desc) <= 3
		then 'Yes'
		else 'No'
	end as top3
from bookings;

-- Rank
select
	booking_id,
	listing_name,
	neighbourhood_group,
	neighbourhood,
	price,
	row_number() over(order by price desc) as overall_price_rank,
	rank() over(order by price desc) as overall_price_rank_with_rank,
	row_number() over(partition by neighbourhood_group order by price desc) as neigh_group_price_rank,
	rank() over(partition by neighbourhood_group order by price desc) as neigh_group_price_with_rank
from bookings;

-- Dense_Rank
select
	booking_id,
	listing_name,
	neighbourhood_group,
	neighbourhood,
	price,
	row_number() over(order by price desc) as overall_price_rank,
	rank() over(order by price desc) as overall_price_rank_with_rank,
	dense_rank() over(order by price desc) as overall_price_rank_with_dense_rank
from bookings;

-- Lag by 1 period
select
	booking_id,
	listing_name,
	host_name,
	price,
	last_review,
	lag(price) over(partition by host_name order by last_review)
from bookings;

select * from bookings;

-- Lag by 2 periods
select
	booking_id,
	listing_name,
	host_name,
	price,
	last_review,
	lag(price,2) over(partition by host_name order by last_review)
from bookings;

-- Lead by 1 period
select
	booking_id,
	listing_name,
	host_name,
	price,
	last_review,
	lead(price) over(partition by host_name order by last_review)
from bookings;

-- Lead by 2 periods
select
	booking_id,
	listing_name,
	host_name,
	price,
	last_review,
	lead(price,2) over(partition by host_name order by last_review)
from bookings;

-- Top 3 with subquery to select only the 'Yes' values in the top_3
select * from (
	select
		booking_id,
		listing_name,
		neighbourhood_group,
		neighbourhood,
		price,
		row_number() over(order by price desc) as overall_price_rank,
		row_number() over(partition by neighbourhood_group order by price desc) as neigh_group_price_rank,
		case
			when row_number() over(partition by neighbourhood_group order by price desc) <= 3
			then 'Yes'
			else 'No'
		end as top_3
	from bookings
	) as a
where top_3='Yes';