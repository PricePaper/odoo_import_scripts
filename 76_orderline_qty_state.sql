UPDATE sale_order_line set qty_to_invoice=q.product_uom_qty FROM (SELECT product_uom_qty FROM sale_order_line) AS q WHERE sale_order_line.id = q.id;;
