UPDATE sale_order SET release_date=q.date_order,deliver_by=q.date_order,effective_date=q.date_order,validity_date=q.date_order,commitment_date=q.date_order FROM (SELECT id,date_order FROM sale_order) AS q WHERE sale_order.id = q.id AND state = 'draft';
UPDATE sale_order_line l SET create_date = so.date_order FROM sale_order so WHERE l.order_id = so.id AND l.state = 'draft';
