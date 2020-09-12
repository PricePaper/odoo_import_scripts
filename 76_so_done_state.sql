UPDATE sale_order SET confirmation_date=q.date_order,release_date=q.date_order,deliver_by=q.date_order,effective_date=q.date_order,validity_date=q.date_order,commitment_date=q.date_order FROM (SELECT id,date_order FROM sale_order) AS q WHERE sale_order.id = q.id;
UPDATE sale_order SET state = 'done';
