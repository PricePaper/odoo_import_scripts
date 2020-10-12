UPDATE product_supplierinfo SET delay = res_partner.delay FROM res_partner WHERE product_supplierinfo.name = res_partner.id;
