



with recent_actief as(
	select 
	ssp.Specialist
	from dbo.Sessie as s
	inner join dbo.Workflow as w on s.ID = w.ID
	inner join dbo.SessieSpecialisten as ssp on s.ID = ssp.Sessie
	where w.CurrentStateName in ('Uitgevoerd', 'Uitvoering', 'Wacht op uitvoering')
	and s.StartTijd between dateadd(year, -1, getdate()) and dateadd(month, 3, getdate())
	group by ssp.Specialist
)
 select 
 sp.ID as specialist_id
 , AgbCode
 from dbo.Specialist as sp
 inner join dbo.Gebruiker as g on sp.ID = g.ID
 inner join recent_actief as ra on sp.ID = ra.Specialist
 where 1=1
 and sp.AgbCode is not null 
 and sp.AgbCode <> ''