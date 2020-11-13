UPDATE sale_order_line set qty_delivered=q.product_uom_qty, qty_to_invoice=q.product_uom_qty FROM (SELECT id, product_uom_qty FROM sale_order_line) AS q WHERE sale_order_line.id = q.id;
