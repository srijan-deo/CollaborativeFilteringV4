SELECT f.mbr_lic_type, f.mbr_state, f.mbr_nbr, f.mbr_email
FROM `cprtpr-dataplatform-sp1`.usviews.v_us_member_fact f
left join `cprtpr-dataplatform-sp1`.usviews.v_us_bids_fact b
ON f.mbr_nbr = b.buyer_nbr
and b.auc_dt between '2025-07-13' and '2025-10-13'
WHERE mbr_mbrshp_type_cd IN ('BASIC', 'PREMIER')
  AND mbr_status = 'A'
  AND mbr_site_status_cd = 'A'
 and bid_id is null
 and mbr_country = 'USA' limit 5000