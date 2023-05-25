with activity as
(
    select start_week, num_shifts, {{ ref('postless_weeks_shifts') }}.facility_id,
    weeks_in_between,
    case
        when (start_week=first_open_week) then 'first_week'
        when weeks_in_between=1 then 'returned'
        when weeks_in_between > 1 then 'resurrected'
    end as agent_state,
    num_shifts - lag(num_shifts) over (partition by {{ ref('postless_weeks_shifts') }}.facility_id order by start_week) as change,
    lag(num_shifts) over (partition by {{ ref('postless_weeks_shifts') }}.facility_id order by start_week) as previous,

    case
        when 
            (lead(weeks_in_between) over (partition by {{ ref('postless_weeks_shifts') }}.facility_id order by start_week) > 1) OR 
            (({{ ref('postless_weeks_shifts') }}.start_week={{ ref('hcf_first_last_shift_open_mod') }}.last_open_week))
            then 1
        else 0
    end as churning,
    
    case
        when (change > 0) and (agent_state='returned') then change
        else 0
    end as expansion,
    
    case
        when (change < 0) and (agent_state='returned') then change
        else 0
    end as contraction,
    
    case
        when (agent_state='returned') and (num_shifts > previous) then previous
        when (agent_state='returned') and (num_shifts < previous) then num_shifts
        when (agent_state='returned') and (num_shifts=previous) then num_shifts
        else 0
    end as retained

    from {{ ref('postless_weeks_shifts') }} left join
    {{ ref('hcf_first_last_shift_open_mod') }} 
    on {{ ref('postless_weeks_shifts') }}.facility_id = {{ ref('hcf_first_last_shift_open_mod') }}.facility_id
)


select start_week,
sum(case when agent_state='first_week' then num_shifts else 0 end) as new_shifts,
sum(case when agent_state='resurrected' then num_shifts else 0 end) as resurrected_shifts,
lag(sum(case when churning=1 then num_shifts else 0 end)) over (order by start_week) as churned_shifts,
sum(expansion) as expansion_shifts,
-sum(contraction) as contraction_shifts,
sum(retained) as retained_shifts,
retained_shifts+churned_shifts+contraction_shifts as previous_time_period_shifts,
(new_shifts+resurrected_shifts+expansion_shifts-contraction_shifts-churned_shifts) / (previous_time_period_shifts+0.0000001) as growth_rate,
case when previous_time_period_shifts > 0 then retained_shifts/previous_time_period_shifts else 0 end as gross_retention,
from activity
group by 1
order by start_week desc
