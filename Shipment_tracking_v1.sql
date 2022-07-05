USE [BusinessIntelligenceProd]
GO
/****** Object:  StoredProcedure [dbo].[TMS_SHIPMENT_V1_PROCEDURE]    Script Date: 7/5/2022 3:43:46 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[TMS_SHIPMENT_V1_PROCEDURE]
AS

BEGIN
  SET NOCOUNT ON;
  SET ANSI_WARNINGS OFF;
  --TRUNCATE TABLE [BusinessIntelligenceProd].[dbo].[TMS_SHIPMENTS_V1]

DECLARE @start_date date = '2022-01-01';
DECLARE @end_date date = GETDATE();


WITH 
tracking_shipment_assign_3pl as (
	SELECT
		s.ordercode ,
		ts.*,
		ROW_NUMBER() OVER(PARTITION BY ts.status,s.ordercode Order by s.ordercode DESC) as rn
	FROM [databaseRepl].dbo.TrackingShipments ts 
	LEFT JOIN [databaseRepl].dbo.shipments s on ts.shipmentid = s.id
	WHERE 
			(ts.Status = 'Active' or ts.Status = '3pls_picking') and
			(ts.Is3Pls = '1') and
			ts.createdtime >='2022-01-01'
),
tracking_shipment_pickup_3pl as (
	SELECT
		s.ordercode ,
		ts.*,
		ROW_NUMBER() OVER(PARTITION BY ts.status,s.ordercode Order by s.ordercode DESC) as rn
	FROM [databaseRepl].dbo.TrackingShipments ts 
	LEFT JOIN [databaseRepl].dbo.shipments s on ts.shipmentid = s.id
	WHERE  
		ts.Status = 'Pickuped' and
		(ts.Is3Pls = '1') and
		ts.createdtime >='2022-01-01'
),
tracking_shipment_return_3pl as (
	SELECT
		s.ordercode ,
		ts.*,
		ROW_NUMBER() OVER(PARTITION BY ts.status,s.ordercode Order by s.ordercode DESC) as rn
	FROM [databaseRepl].dbo.TrackingShipments ts 
	LEFT JOIN [databaseRepl].dbo.shipments s on ts.shipmentid = s.id
	WHERE 
			ts.Status = 'Returned'  and
			(ts.Is3Pls = '1') and
			ts.createdtime >= '2022-01-01'
),

tracking_status_3pl as(
	SELECT
		s.ordercode ,
		ts.*,
		ROW_NUMBER() OVER(PARTITION BY s.ordercode,ts.Is3Pls Order by ts.id DESC) as rn
	FROM [databaseRepl].dbo.TrackingShipments ts 
	LEFT JOIN [databaseRepl].dbo.shipments s on ts.shipmentid = s.id
	WHERE
		(ts.Is3Pls = '1') and
		ts.createdtime >='2022-01-01'
),


log_shipment_closed AS (
SELECT id
		, shipperId
		, GroupShipmentId
		, ShipmentId
		, ModifiedTime AS closed_time
		, ROW_NUMBER() OVER (PARTITION BY shipmentid ORDER BY ModifiedTIME ASC) rn
		
FROM  databaseRepl.dbo.TrackingShipments
WHERE Status = 'Completed'  and DataStatus !=2
),

sla_detail AS
(SELECT
	  u.Name as 'shipper_name'
	  , gr.Partner as 'PartnerName'
	  , sr.PartnerId
	  , gr.SenderPrimaryPhone
	  , gr.SenderName
	  , gr.SenderId
	  , IIF(sr.PickupCity is not null, sr.PickupCity, sr.ReceiverCity) as 'City'
	  , sr.CustomerOrderCode as 'ma_package_khach_hang'
      , iif (sr.ordercode IS NULL, s.ordercode, sr.OrderCode) as 'ma_package_'
	  , COALESCE(sr.CreatedTime, s.createdtime) as 'partner_request_datetime'
	  , s.CreatedTime as 'ngay_gan_don'
	  , ts2.CreatedTime as 'delivery_fail_datetime'	  
	  , ts2.ReasonSendUnsuccess as 'delivery_fail_reason'	  
	  , ts5.CreatedTime as 'delivered_datetime'
	  , ts6.CreatedTime as 'createdtrip_datetime'
	  , s.CreatedTime as 'assigntrip_datetime'
	  ,CASE 
			when s.status = 'Active' and s.ShipmentType = 'Pickup' then N'04. Đang thu gom'
			when s.status = 'Active' OR s.status = 'Onway' then N'09. Đang giao'
			when s.status = 'Completed' then N'10. Giao TC (FINAL)'
			when s.status = 'SendUnsuccessful' then N'11. Giao KTC'
			when s.status = 'Returned' and s.State = 'ToReturn' and sr.IsReturnable = 1 then N'13b. Xác nhận có thể hoàn'
			when s.status = 'Returned' and s.State = 'ToReturn' and sr.State = 'NeedToReDeliver' then N'12. Yêu cầu phát lại'			
			when s.status = 'Returned' and s.State = 'ToReturn' then N'12. Nhập kho chờ phát lại'	
			when s.status = 'StorageToReturn' and s.State = 'Returning' then N'13. Nhập kho chờ hoàn lại'			
			when s.status = 'Returned' and s.State = 'ConfirmReturned' then N'13. Đã hoàn shop (FINAL)'
			when s.ShipmentType = 'Return' and s.status = 'Returned' then N'14. Đã hoàn shop (FINAL)'
			when s.ShipmentType = 'Return' and s.status = 'Returning' then N'14. Đang hoàn'		
			when s.ShipmentType = 'Return' and s.status = 'ReturnUnsuccessful' then N'14. Hoàn KTC'			
			when s.status = 'Returned' and s.State = 'Returning'  then N'14. Đang hoàn'			
			when s.status = 'Returned' then N'14b. Đơn giao đang hoàn'	
			when sr.ServiceType = 'FM' and sr.state = 'confirm_handed_over' then N'08c. Xác nhận đã bàn giao SC (FINAL)'
			when sr.ServiceType = 'FM' and s.Status = 'Pickuped' and sr.state = 'handed_over' then N'08b. Đã bàn giao SC'	
			when sr.ServiceType = 'FM' and s.Status = 'Pickuped' and s.State = 'StorageToDelivery' and sr.StoreId in (5326,5327) and sr.CreatedTime < '2020-09-28'
			then N'08b. Đã bàn giao SC (FINAL)'
			when (s.status = 'Pickuped' and (s.State = 'StorageToDelivery' or s.state = 'handed_over'))
			then N'08. Nhập kho chờ giao - shop'	
			when (sr.status = 'New' AND s.Status IS NULL AND sr.IsShipped = 1) 
			OR (sr.status = 'AddressUpdated' and sr.PickupShipmentCreated = 0 AND sr.IsShipped = 1) then N'08. Nhập kho chờ giao - hub'
			when s.status = 'Pickuped' then N'05. Thu gom TC'

			when s.status = 'CanceledPickup' then N'04. Đang thu gom - lại'
			when s.status = 'PickupUnsuccessful' and s.State = 'PickupFailed' and sr.PickupShipmentCreated = 1 then N'07. Thu gom KTC - huỷ (FINAL)'
			when s.status = 'PickupUnsuccessful' and (s.ShipmentType = 'Pickup' OR s.ShipmentType IS NULL) then N'06. Thu gom KTC'			
			--when s.status = 'PickupUnsuccessful' and s.ShipmentType IS NULL then N'PICK UP FAILED - Tuyến giao'			
			when sr.status = 'AddressUpdated' and sr.PickupShipmentCreated = 0 then N'03. Chưa thu gom'		
			--when sr.status = 'AddressUpdated' then N'09. Ghép tuyến chờ giao'
			when (sr.status = 'New' AND s.Status IS NULL AND sr.IsShipped = 0) 
			OR (sr.status = 'AddressUpdated' and sr.PickupShipmentCreated = 0) 
			then N'03. Chưa thu gom'
			when sr.ServiceType = 'FMRT' and sr.status = 'AddressUpdated' and sr.IsShipped = 0 then N'14. Yêu cầu chuyển hoàn mới'				
			when sr.ServiceType = 'FMRT' and sr.status = 'AddressUpdated' and sr.IsShipped = 1 then N'14. Nhập kho chờ hoàn'				
			when sr.status = 'AddressUpdated' then N'08. Nhập kho chờ giao - hub'				
			when s.status = 'Canceled' then N'02. Huỷ bởi '
			ELSE N'CHƯA THÔNG BÁO RIDER' 
		END AS 'status'

	  , s.State as sm_table_state
	  , s.Status sm_table_status
	  , sr.IsReturnable
	  , sr.PickupCity
	  , sr.PickupDistrict
	  , sr.PickupWard
	  , sr.PickupRawAddress
	  , sr.ReceiverDistrict
	  , sr.ReceiverRawAddress
	  , sr.ReceiverName
	  , sr.ReceiverWard
	  , sr.DeliveryNote
	  , CASE WHEN sr.servicetype LIKE N'%6%'  THEN 'Sameday'
		  WHEN sr.servicetype LIKE N'%24%'  THEN 'Nextday'
		  WHEN sr.servicetype IS NULL  THEN 'Sameday'
		  WHEN sr.servicetype LIKE N'%2%'  THEN 'Express'
		  ELSE sr.ServiceType
	   END AS service_express
	  , sr.ServiceType
	  , sr.ShippedTime
	  , sr.COD
	  , sr.CouponCode
	  , sr.CostDiscount
	  , sr.Weight
	  , sr.PackageQuantity
	  , sr.MasterCode AS sku_code
	  , sr.NoReturnAttempt
	  , sr.PackageDescription
	  , sr.ReceiverPrimaryPhone
	  , ts8.CreatedTime as 'Picked_up_datetime'
	  , u2.Name as 'Picked_up_Shipper'
	  , ts9.CreatedTime as 'Pickup_unsuccess_datetime'
	  , sr.LastPickupAttemptDateTime as 'Pickup_last_failattempt_datetime'
	  , sr.NoDeliveryAttempt
	  , sr.NoPickupAttempt
	  , sr.ReceiverCity
	  , ts10.CreatedTime as 'inbounded_time_for_return'
	  , ts10.lead_created_time AS 'outbounded_time_for_return'
	  , ts11.CreatedTime as 'pickupfailed_datetime'
	  , ts12.CreatedTime as 'Pickup_pre_last_failattempt_datetime'
	  , ts12.ReasonPickUpUnsuccessful
	  , ps.Name as 'store_name'
	  , ts13.CreatedTime as 'Returned_datetime'
	  , ts14.CreatedTime as 'Canceled_datetime'
	  , ts15.CreatedTime as 'Compensation_datetime'
	  , ts16.CreatedTime as 'PU_Inbound_DateTime'
	  , ts16.Note as 'PU_Inbound_Note'
	  , sr.Id as 'ShipmentId'
	  , pt.Name as 'Pickup_Store'
	  , pt2.Name as 'Delivery_Store'
	  , case
			WHEN s.CreatedTime is null  THEN sr.CreatedTime
			WHEN sr.CreatedTime > s.CreatedTime  THEN sr.CreatedTime
			else s.CreatedTime
	   end as 'last_updated_datetime'
	  ,sr.State AS sr_state
	  ,sr.Status as sr_status
	  ,lsc.closed_time
	  ,sr.id as shipmentrequestid
	  ,sta1.CreatedTime AS send_handing_over_time
	  ,sta2.CreatedTime AS send_handed_over_time
	  ,sta3.modifiedtime AS start_returning_time
	  ,sta4.modifiedtime AS completed_returned_time
	  ,sta5.CreatedTime AS completed_delivery_time
	  ,sta6.ModifiedTime AS start_delivery_time
	  ,sta8.ModifiedTime AS second_attemp_delivery_time
	  ,handing_over.ModifiedTime AS max_handing_over_time
	  ,sta10.CreatedTime AS start_transfer_time_to_return
	  ,sta10.lead_created_time AS end_transfer_time_to_return
	  ,CASE WHEN s.status = 'Returned'  AND sr.IsReturnable = 1  THEN sta10.CreatedTime END AS bat_dau_luan_chuyen_tra_time
	  ,sta16.CreatedTime AS inbounded_time_sc_for_return
	  ,sta16.lead_created_time AS outbound_time_sc_for_return
	  ,sta12.CreatedTime AS inbounded_sc_time_for_send
	  ,sta11.lead_created_time AS outbounded_time_sc_for_send
	  ,sta15.CreatedTime AS inbounded_time_for_send
	  ,lt.Status AS updated_status
	  ,CASE WHEN s.CustomerOrderCode IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY s.CustomerOrderCode ORDER BY s.CreatedTime DESC) ELSE NULL END as RowNo
	  --3pl
	  	,CASE
		WHEN tspl14.Is3Pls = 1 and lower(u.name) LIKE '%ghn%' THEN 'GHN'
		WHEN tspl14.Is3Pls = 1 and lower(u.name) LIKE '%ninjavan%' THEN 'NJV'
		WHEN tspl14.is3Pls = 1 THEN u.name
		ELSE NULL 
	END as 'partner_3pl'
	,CASE WHEN tspl14.Is3Pls = 1 THEN ps.city ELSE NULL END as 'pickup_city_3pl'
	,CASE WHEN tspl14.Is3Pls = 1 THEN sr.ReceiverCity ELSE NULL END as 'delivery_city_3pl'
	,s.id as shipmentids
	,s.TrackingNumberId as 'ordercode_3pl'
	,tspl14.Is3Pls
	,tspl14.CreatedTime as 'assign_datetime_3pl'
	,tspl15.CreatedTime as 'pickuped_datetime_3pl'
	,tspl16.CreatedTime as 'returned_datetime_3pl'
	,ts.Status as 'raw_status_3pl'
	,spl.status_3pl as 'status_3pl'
FROM  databaseRepl.dbo.shipmentrequests sr 

	  JOIN databaseRepl.dbo.GroupShipmentRequests gr ON sr.GroupShipmentId = gr.Id

	  FULL OUTER JOIN databaseRepl.dbo.shipments s ON sr.Id = s.ShipmentRequestId 

	  FULL OUTER JOIN databaseRepl.dbo.shipments s2 ON sr.Id = s2.ShipmentRequestId and s2.ShipmentType = 'Pickup' --and s2.Status = 'Pickuped'

	  LEFT JOIN databaseRepl.dbo.groupshipments g ON s.groupshipmentid = g.id

	  LEFT JOIN databaseRepl.dbo.shipmentusers su ON su.groupshipmentid = s.GroupShipmentId and su.isselected = 1

	  LEFT JOIN databaseRepl.dbo.users u ON su.userid = u.id	 	  

	  LEFT JOIN (SELECT ma_package_, CreatedTime, shipment_type, ReasonSendUnsuccess
					FROM  BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG
					WHERE Status = 'SendUnsuccessful' AND row_number_by_modified_time_descending=1) AS ts2
			ON ts2.ma_package_ = sr.OrderCode

	  LEFT JOIN (SELECT * FROM  log_shipment_closed WHERE rn=1) lsc ON s.id = lsc.shipmentid
	  
	  LEFT JOIN BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG ts5 
			ON sr.OrderCode = ts5.ma_package_ AND ts5.Status = 'Completed' AND ts5.State IS NULL AND ts5.row_number_by_completed_delivery = 1 -- ts5.ShipperId = su.UserId AND
	  
	  
	  LEFT JOIN BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG ts6
			ON sr.OrderCode = ts6.ma_package_ AND ts6.Status = 'New' AND ts6.State IS NULL AND ts6.shipment_type = 'Delivery' AND ts6.row_number_by_modified_time_descending = 1
	  
	  LEFT JOIN BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG ts7
			ON sr.OrderCode = ts7.ma_package_ AND ts7.Status = 'Onway' AND ts7.State IS NULL AND ts6.shipment_type = 'Delivery' AND ts7.row_number_by_modified_time = 1

	  LEFT JOIN BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG ts8
			ON sr.OrderCode = ts8.ma_package_ AND ts8.Status = 'Pickuped' AND  ts8.row_number_by_modified_time_descending = 1

	  LEFT JOIN BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG ts9
			ON sr.OrderCode = ts9.ma_package_ AND ts9.Status = 'PickupUnsuccessful' AND  ts9.row_number_by_modified_time_descending = 1 
	
	  LEFT JOIN (SELECT * FROM  (
								SELECT ma_package_
									, CreatedTime
									, lead_created_time
									, ROW_NUMBER() OVER (PARTITION BY shipmentrequestid ORDER BY CreatedTime DESC) as rn
 								FROM  BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG
								WHERE Status = 'handed_over' AND is_sc_hub = N'Kho hiện tại' AND confirmed_return_time IS NOT NULL
						) a
				WHERE a.rn = 1

				) AS ts10
			ON ts10.ma_package_ = sr.OrderCode
	  


	  LEFT JOIN BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG handing_over 
			ON handing_over.ma_package_ = sr.OrderCode and handing_over.Status = 'handing_over'  AND handing_over.row_number_by_modified_time_descending = 1

	  LEFT JOIN databaseRepl.dbo.users u2 ON u2.id = ts8.ShipperId

	  LEFT JOIN BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG ts11 
			ON ts11.ma_package_ = sr.OrderCode and ts11.state = 'PickupFailed' AND ts11.row_number_by_modified_time_descending = 1		

	  
	  LEFT JOIN BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG ts12 
			ON ts12.ma_package_ = sr.OrderCode and ts12.Status = 'PickupUnsuccessful'  AND ts12.row_number_by_modified_time_descending = 1


	  LEFT JOIN databaseRepl.dbo.PartnerStores ps 
			ON sr.StoreId = ps.Id

	  LEFT JOIN BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG ts13 
				ON ts13.ma_package_ = sr.OrderCode AND ts13.Status = 'Returned' and ts13.Note = N'Đơn hàng chuyển hoàn thành công'

	  LEFT JOIN databaseRepl.dbo.TrackingShipments ts14 ON ts14.ShipmentRequestid = s.Id and ts14.Status = 'Canceled'			
	  LEFT JOIN databaseRepl.dbo.TrackingShipments ts15 ON ts15.ShipmentRequestid = s.Id and ts15.State = 'compensation'	
		
	  LEFT JOIN BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG ts16 
			ON ts16.ma_package_ = sr.OrderCode AND ts16.state = 'StorageToDelivery'

	  LEFT JOIN databaseRepl.dbo.PartnerStores pt ON pt.id = sr.PickupStoreId
	  LEFT JOIN databaseRepl.dbo.PartnerStores pt2 ON pt2.id = sr.DeliveryStoreId

		--LEFT JOIN (SELECT  databaseRepl.dbo.TrackingShipments

		-- Get handing_over time
	  LEFT JOIN (SELECT ma_package_, status, CreatedTime, state
						FROM  BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG WHERE Status = 'handing_over' AND row_number_by_modified_time = 1) sta1
			ON sta1.ma_package_ = sr.OrderCode

		-- Get handed_over time
	  LEFT JOIN (SELECT ma_package_, status, CreatedTime, state
						FROM  BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG WHERE Status = 'handed_over' AND row_number_by_modified_time = 1) sta2
			ON sta2.ma_package_ = sr.OrderCode

		-- Get returning_over time
	  LEFT JOIN (SELECT ma_package_, status, modifiedtime , state
						FROM  BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG 
						WHERE Status = 'Returning' AND shipment_type = 'Return' AND row_number_by_modified_time_descending = 1) sta3
			ON sta3.ma_package_ = sr.OrderCode

		-- Get returned_over time
	  LEFT JOIN (SELECT ma_package_, status, modifiedtime, state
						FROM  BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG 
						WHERE Status = 'Returned' AND shipment_type = 'Return' AND row_number_by_modified_time = 1) sta4
			ON sta4.ma_package_ = sr.OrderCode

		--left join (SELECT ma_package_khach_hang, status, modifiedtime, state
		--				FROM  BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG 
		--				WHERE Status = 'ReturnUnsuccessful' AND shipment_type = 'Return' AND row_number_by_modified_time = 1) sta41
		--	ON sta4.ma_package_khach_hang = sr.CustomerOrderCode

	  LEFT JOIN (SELECT ma_package_khach_hang, ma_package_, status, CreatedTime, state
					FROM  BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG 
					WHERE Status = 'Completed' AND state IS NULL AND row_number_by_completed_delivery = 1) sta5
			ON sta5.ma_package_ = sr.OrderCode
		
	  LEFT JOIN (SELECT ma_package_, status, modifiedtime, shipment_type, state
				   FROM  BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG 
				   WHERE ((Status = 'Onway' OR status = 'Active') AND shipment_type = 'Delivery') AND row_number_by_modified_time = 1) sta6
			ON sta6.ma_package_ = sr.OrderCode
		
	  LEFT JOIN (SELECT ma_package_, status, modifiedtime, shipment_type, state
				   FROM  BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG 
				   WHERE Status = 'SendUnsuccessful' AND row_number_by_modified_time = 1) sta7
			ON sta7.ma_package_ = sr.OrderCode

	  LEFT JOIN (SELECT ma_package_, status, modifiedtime, shipment_type, state
				   FROM  BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG 
				   WHERE Status = 'SendUnsuccessful' AND row_number_by_modified_time = 2) sta8
			ON sta8.ma_package_ = sr.OrderCode

	  LEFT JOIN (SELECT ma_package_, status, modifiedtime, shipment_type, state, service_type, CreatedTime
				   FROM  BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG 
				   WHERE row_number_by_shipment_status = 1) lt

			ON lt.ma_package_ = sr.OrderCode

		--luồng shipments luân chuyển trả
		--LEFT JOIN (SELECT ma_package_khach_hang, status, CreatedTime, shipment_type, state
		--			FROM  BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG
		--			WHERE Status = 'handed_over' AND row_number_by_modified_time_descending=1 AND ) AS sta9
		--	ON sta9.ma_package_khach_hang = sr.CustomerOrderCode
		
	  LEFT JOIN (SELECT ma_package_, CreatedTime, lead_created_time
					FROM  BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG
					WHERE Status = 'handing_over' AND row_number_by_modified_time_descending=1 AND confirmed_return_time IS NOT NULL
					) AS sta10
			ON sta10.ma_package_ = sr.OrderCode

	  LEFT JOIN (
					SELECT *, ROW_NUMBER() OVER (PARTITION BY shipmentrequestid, status ORDER BY lead_created_time ASC) rn
					FROM  (
					SELECT 
						status,
						lead_created_time, 
						CreatedTime, 
						ShipmentRequestid
					FROM  BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG
					where is_sc_hub = N'Kho SC'
					--and ma_package_ = 'AHY6ND4T'
					AND CreatedTime > '2022-01-01'
					AND status = 'handed_over'
					) a

				) sta11

			ON sta11.ShipmentRequestid = sr.id AND sta11.rn = 1

	  LEFT JOIN (
					SELECT *, ROW_NUMBER() OVER (PARTITION BY shipmentrequestid, status ORDER BY modifiedtime ASC) rn
					FROM  (
					SELECT 
						status,
						ModifiedTime, 
						CreatedTime, 
						ShipmentRequestid,
						CASE WHEN status = 'handed_over' AND Note LIKE N'%SC%'  THEN N'Kho SC' 
								WHEN status = 'handed_over' AND Note NOT LIKE N'%SC%'  THEN N'Kho hiện tại' 
								WHEN status <> 'handed_over'  THEN NULL
							END AS is_sc_hub
					FROM  BusinessIntelligenceProd.dbo.SHIPMENTS_TRACKING_AGG
					where ShipmentRequestid IS NOT NULL
					AND CreatedTime > '2022-01-01'
					AND status = 'handed_over'
					) a
					WHERE a.is_sc_hub = N'Kho SC'

				) sta12

			ON sta12.ShipmentRequestid = sr.id AND sta12.rn = 1

	  LEFT JOIN [BusinessIntelligenceProd].[dbo].[HANDING_OVER_SC] sta13
			ON sta13.ShipmentRequestid = sr.id AND sta13.rn_handing_over = 1 AND sta13.segment = 'Handing over'

		--LEFT JOIN [BusinessIntelligenceProd].[dbo].[HANDING_OVER_SC] sta14
		--	ON sta14.ShipmentRequestid = sr.id AND sta14.rn_handing_over = 1 AND sta14.segment = 'Fail Delivery' AND sta14.status = 'handing_over'

	  LEFT JOIN (SELECT *, ROW_NUMBER() OVER (PARTITION BY ShipmentRequestid ORDER BY CreatedTime ASC) rn
					FROM  (
							SELECT status,
								   ShipmentRequestid,
								   CreatedTime,
								   lead_created_time
							FROM   [BusinessIntelligenceProd].[dbo].[HANDING_OVER_SC]
							WHERE is_sc_hub = N'Kho hiện tại' AND segment = 'Fail Delivery/Return'
						) a
					) sta15
		  ON sta15.ShipmentRequestid = sr.id AND sta15.rn = 1

	  LEFT JOIN (SELECT *, ROW_NUMBER() OVER (PARTITION BY ShipmentRequestid ORDER BY CreatedTime DESC) rn
					FROM  (
							SELECT status,
								   ShipmentRequestid,
								   CreatedTime,
								   lead_created_time
							FROM   [BusinessIntelligenceProd].[dbo].[HANDING_OVER_SC]
							WHERE is_sc_hub = N'Kho SC' AND segment = 'Fail Delivery/Return'
									AND min_created_time_by_sm IS NOT NULL
						) c
					) sta16
		  ON sta16.ShipmentRequestid = sr.id AND sta16.rn = 1

---3PL
    left join (SELECT * FROM tracking_status_3pl WHERE rn = 1) ts on ts.OrderCode = sr.ordercode
	left join (SELECT * FROM tracking_shipment_assign_3pl WHERE rn = 1) tspl14 on tspl14.OrderCode = sr.ordercode
	left join (SELECT * FROM tracking_shipment_pickup_3pl WHERE rn = 1) tspl15 on tspl15.OrderCode = sr.ordercode
	left join (SELECT * FROM tracking_shipment_return_3pl WHERE rn = 1) tspl16 on tspl16.OrderCode = sr.ordercode
	LEFT JOIN (SELECT DISTINCT ts_status, status_3pl from [BusinessIntelligenceProd].[dbo].[STATUS_3PL]) spl
		on ts.status  = spl.ts_status

	--sr.PartnerId in (1,1028,2,1018) 
	--and (s.ShipmentType is null or s.ShipmentType = 'Delivery')	   
WHERE
	sr.PartnerId not in(1037,1067,1066,1053)
	AND (sr.DataStatus is null or sr.DataStatus != 2)
	and (s.DataStatus is null or s.DataStatus != 2) 
	and sr.Status != 'WrongRequest' 	    
	and sr.createdtime >= '2022-01-01' 
	--AND s.OrderCode = 'AHY6ND4T'
	--AND s.OrderCode = 'C66L3N0Z'
	  --and s.Status = 'Completed'
		   --and (s.ShipmentType != 'Pickup' or s.ShipmentType is null)	
),

--insert into TrackingActionUsers (Executor,Description,Type,DataStatus,CreatedTime,ModifiedTime,ActionName, reason) 
--values ('data_reader', N'Rider_Performance_Bonus_6h_Delivery','Report',0,SYSDATETIMEOFFSET(),SYSDATETIMEOFFSET(),'Report', 'Start: ' + LEFT(CONVERT(VARCHAR, @Starttime, 120), 10) + ' - End: ' + LEFT(CONVERT(VARCHAR, @Endtime, 120), 10))

final_sla AS (

SELECT *
		,CASE WHEN service_express = 'Sameday' AND partner_request_datetime < DATEADD(minute, 630, CONVERT(DATETIME, CONVERT(DATE, partner_request_datetime)))  THEN DATEADD(minute, 660, CONVERT(DATETIME, CONVERT(DATE, partner_request_datetime)))
			  WHEN service_express = 'Sameday' AND partner_request_datetime BETWEEN DATEADD(minute, 630, CONVERT(DATETIME, CONVERT(DATE, partner_request_datetime))) AND DATEADD(minute, 60*15.9999, CONVERT(DATETIME, CONVERT(DATE, partner_request_datetime)))  THEN DATEADD(minute, 1020, CONVERT(DATETIME, CONVERT(DATE, partner_request_datetime)))
			  WHEN service_express = 'Sameday' AND partner_request_datetime > DATEADD(minute, 960, CONVERT(DATETIME, CONVERT(DATE, partner_request_datetime)))  THEN DATEADD(minute, 1440, CONVERT(DATETIME, CONVERT(DATE, partner_request_datetime)))
			  WHEN service_express = 'Nextday' AND partner_request_datetime < DATEADD(minute, 960, CONVERT(DATETIME, CONVERT(DATE, partner_request_datetime)))  THEN DATEADD(minute, 1020, CONVERT(DATETIME, CONVERT(DATE, partner_request_datetime)))
			  WHEN service_express = 'Nextday' AND partner_request_datetime > DATEADD(minute, 960, CONVERT(DATETIME, CONVERT(DATE, partner_request_datetime)))  THEN DATEADD(minute, 1440, CONVERT(DATETIME, CONVERT(DATE, partner_request_datetime)))
		 END AS expected_pickup_time
		,CASE WHEN service_express = 'Sameday' AND picked_up_datetime < DATEADD(minute, 60*10.5, CONVERT(DATETIME, CONVERT(DATE, picked_up_datetime)))  THEN DATEADD(minute, 1440, CONVERT(DATETIME, CONVERT(DATE,picked_up_datetime)))
			  WHEN service_express = 'Sameday' AND picked_up_datetime > DATEADD(minute, 660, CONVERT(DATETIME, CONVERT(DATE, picked_up_datetime)))  THEN DATEADD(minute, 60*36, CONVERT(DATETIME, CONVERT(DATE, picked_up_datetime)))
			  WHEN service_express = 'Nextday' AND picked_up_datetime < DATEADD(minute, 60*24, CONVERT(DATETIME, CONVERT(DATE,picked_up_datetime)))  THEN DATEADD(minute, 60*48, CONVERT(DATETIME, CONVERT(DATE, picked_up_datetime)))
		 END AS expected_delivery_rod_time
		 ,CASE WHEN service_express = 'Sameday' AND picked_up_datetime < DATEADD(minute, 60*10.5, CONVERT(DATETIME, CONVERT(DATE, picked_up_datetime)))  THEN DATEADD(minute, 60*11, CONVERT(DATETIME, CONVERT(DATE,picked_up_datetime)))
			   WHEN service_express = 'Sameday' AND picked_up_datetime BETWEEN DATEADD(minute, 60*10.5, CONVERT(DATETIME, CONVERT(DATE, picked_up_datetime))) AND DATEADD(minute, 60*15.99999, CONVERT(DATETIME, CONVERT(DATE, picked_up_datetime)))  THEN DATEADD(minute, 60*18, CONVERT(DATETIME, CONVERT(DATE,picked_up_datetime)))
			   WHEN service_express = 'Sameday' AND picked_up_datetime > DATEADD(minute, 60*16, CONVERT(DATETIME, CONVERT(DATE, picked_up_datetime)))  THEN DATEADD(minute, 60*32, CONVERT(DATETIME, CONVERT(DATE,picked_up_datetime)))
			   WHEN service_express = 'Nextday' AND picked_up_datetime < DATEADD(minute, 60*16, CONVERT(DATETIME, CONVERT(DATE, picked_up_datetime)))  THEN DATEADD(minute, 60*18, CONVERT(DATETIME, CONVERT(DATE, picked_up_datetime)))
			   WHEN service_express = 'Nextday' AND picked_up_datetime > DATEADD(minute, 60*16, CONVERT(DATETIME, CONVERT(DATE,picked_up_datetime)))  THEN DATEADD(minute, 60*32, CONVERT(DATETIME, CONVERT(DATE, picked_up_datetime)))
		 END AS expected_transfer_time_for_send

		 ,CASE WHEN service_express = 'Sameday' AND start_transfer_time_to_return < DATEADD(minute, 60*10.5, CONVERT(DATETIME, CONVERT(DATE,start_transfer_time_to_return)))  THEN DATEADD(minute, 60*11, CONVERT(DATETIME, CONVERT(DATE,start_transfer_time_to_return)))
			   WHEN service_express = 'Sameday' AND start_transfer_time_to_return BETWEEN DATEADD(minute, 60*10.5, CONVERT(DATETIME, CONVERT(DATE, start_transfer_time_to_return))) AND DATEADD(minute, 60*15.99999, CONVERT(DATETIME, CONVERT(DATE, start_transfer_time_to_return)))  THEN DATEADD(minute, 60*18, CONVERT(DATETIME, CONVERT(DATE,start_transfer_time_to_return)))
			   WHEN service_express = 'Sameday' AND start_transfer_time_to_return > DATEADD(minute, 60*16, CONVERT(DATETIME, CONVERT(DATE, start_transfer_time_to_return)))  THEN DATEADD(minute, 60*32, CONVERT(DATETIME, CONVERT(DATE,start_transfer_time_to_return)))
			   WHEN service_express = 'Nextday' AND start_transfer_time_to_return < DATEADD(minute, 60*16, CONVERT(DATETIME, CONVERT(DATE, start_transfer_time_to_return)))  THEN DATEADD(minute, 60*18, CONVERT(DATETIME, CONVERT(DATE, start_transfer_time_to_return)))
			   WHEN service_express = 'Nextday' AND start_transfer_time_to_return > DATEADD(minute, 60*16, CONVERT(DATETIME, CONVERT(DATE, start_transfer_time_to_return)))  THEN DATEADD(minute, 60*32, CONVERT(DATETIME, CONVERT(DATE, start_transfer_time_to_return)))
		 END AS expected_transfer_time_to_return

		 ,CASE WHEN inbounded_time_for_send < DATEADD(minute, 60*9, CONVERT(DATETIME, CONVERT(DATE, inbounded_time_for_send)))  THEN DATEADD(minute, 60*12, CONVERT(DATETIME, CONVERT(DATE, inbounded_time_for_send)))
			   WHEN inbounded_time_for_send BETWEEN DATEADD(minute, 60*9, CONVERT(DATETIME, CONVERT(DATE, inbounded_time_for_send))) AND DATEADD(minute, 60*15.99999, CONVERT(DATETIME, CONVERT(DATE, inbounded_time_for_send)))  THEN DATEADD(minute, 60*24, CONVERT(DATETIME, CONVERT(DATE,inbounded_time_for_send)))
			   WHEN inbounded_time_for_send > DATEADD(minute, 60*16, CONVERT(DATETIME, CONVERT(DATE, inbounded_time_for_send)))  THEN DATEADD(minute, 60*32, CONVERT(DATETIME, CONVERT(DATE, inbounded_time_for_send)))
		  END AS expected_delivery_time
		,ROW_NUMBER() OVER (PARTITION BY ma_package_ ORDER BY last_updated_datetime DESC) rn

FROM  sla_detail
WHERE (RowNo IS NULL OR RowNo = 1)
)

--SELECT *
--INTO BusinessIntelligenceProd.dbo.TMS_SHIPMENTS_V1
--FROM final_sla
--WHERE rn = 1



MERGE INTO [BusinessIntelligenceProd].[dbo].[TMS_SHIPMENTS_V1] AS tar

USING (

SELECT 
		[shipper_name],
		[PartnerName],
		[City],
		[ma_package_khach_hang],
		[ma_package_],
		[partner_request_datetime],
		[ngay_gan_don],
		[delivery_fail_datetime],
		[delivery_fail_reason],
		[delivered_datetime],
		[createdtrip_datetime],
		[assigntrip_datetime],
		[status],
		[sm_table_state],
		[sm_table_status],
		[IsReturnable],
		[PickupDistrict],
		[PickupWard],
		[PickupRawAddress],
		[ReceiverDistrict],
		[ReceiverRawAddress],
		[ReceiverWard],
		[service_express],
		[ShippedTime],
		[COD],
		[Weight],
		[SenderName],
		[Picked_up_datetime],
		[Picked_up_Shipper],
		[Pickup_unsuccess_datetime],
		[Pickup_last_failattempt_datetime],
		[NoDeliveryAttempt],
		[NoPickupAttempt],
		[ReceiverCity],
		[inbounded_time_for_return],
		[outbounded_time_for_return],
		[pickupfailed_datetime],
		[Pickup_pre_last_failattempt_datetime],
		[ReasonPickUpUnsuccessful],
		[store_name],
		[Returned_datetime],
		[Canceled_datetime],
		[Compensation_datetime],
		[PU_Inbound_DateTime],
		[PU_Inbound_Note],
		[ShipmentId],
		[Pickup_Store],
		[Delivery_Store],
		[Last_updated_datetime],
		[sr_state],
		[sr_status],
		[closed_time],
		[shipmentrequestid],
		[send_handing_over_time],
		[send_handed_over_time],
		[start_returning_time],
		[completed_returned_time],
		[completed_delivery_time],
		[start_delivery_time],
		[second_attemp_delivery_time],
		[max_handing_over_time],
		[start_transfer_time_to_return],
		[end_transfer_time_to_return],
		[inbounded_time_sc_for_return],
		[outbound_time_sc_for_return],
		[inbounded_sc_time_for_send],
		[outbounded_time_sc_for_send],
		[inbounded_time_for_send],
		[expected_pickup_time],
		[expected_delivery_rod_time],
		[expected_transfer_time_for_send],
		[expected_transfer_time_to_return],
		[expected_delivery_time],
		[sku_code],
		[updated_status],
		[PackageQuantity],
		[PackageDescription],
		[SenderPrimaryPhone],
		[ReceiverPrimaryPhone],
		[ReceiverName],
		[partner_3pl],
		[pickup_city_3pl],
		[delivery_city_3pl],
		[ordercode_3pl],
		[Is3Pls],
		[assign_datetime_3pl],
		[pickuped_datetime_3pl],
		[returned_datetime_3pl],
		[raw_status_3pl],
		[status_3pl],
		senderid,
		PartnerId
		
FROM  final_sla
WHERE rn = 1) AS sour 
		
		ON tar.ma_package_ = sour.ma_package_

WHEN MATCHED THEN

	UPDATE SET 
		tar.[shipper_name] = sour.[shipper_name],
		tar.[PartnerName] = sour.[PartnerName],
		tar.[City] = sour.[City],
		tar.[ma_package_khach_hang] = sour.[ma_package_khach_hang],
		--tar.[ma_package_] = sour.[ma_package_],
		tar.[partner_request_datetime] = sour.[partner_request_datetime],
		tar.[ngay_gan_don] = sour.[ngay_gan_don],
		tar.[delivery_fail_datetime] = sour.[delivery_fail_datetime],
		tar.[delivery_fail_reason] = sour.[delivery_fail_reason],
		tar.[delivered_datetime] = sour.[delivered_datetime],
		tar.[createdtrip_datetime] = sour.[createdtrip_datetime],
		tar.[assigntrip_datetime] = sour.[assigntrip_datetime],
		tar.[status] = sour.[status],
		tar.[sm_table_state] = sour.[sm_table_state],
		tar.[sm_table_status] = sour.[sm_table_status],
		tar.[IsReturnable] = sour.[IsReturnable],
		tar.[PickupDistrict] = sour.[PickupDistrict],
		tar.[PickupWard] = sour.[PickupWard],
		tar.[PickupRawAddress] = sour.[PickupRawAddress],
		tar.[ReceiverDistrict] = sour.[ReceiverDistrict],
		tar.[ReceiverRawAddress] = sour.[ReceiverRawAddress],
		tar.[ReceiverWard] = sour.[ReceiverWard],
		tar.[service_express] = sour.[service_express],
		tar.[ShippedTime] = sour.[ShippedTime],
		tar.[COD] = sour.[COD],
		tar.[Weight] = sour.[Weight],
		tar.[SenderName] = sour.[SenderName],
		tar.[Picked_up_datetime] = sour.[Picked_up_datetime],
		tar.[Picked_up_Shipper] = sour.[Picked_up_Shipper],
		tar.[Pickup_unsuccess_datetime] = sour.[Pickup_unsuccess_datetime],
		tar.[Pickup_last_failattempt_datetime] = sour.[Pickup_last_failattempt_datetime],
		tar.[NoDeliveryAttempt] = sour.[NoDeliveryAttempt],
		tar.[NoPickupAttempt] = sour.[NoPickupAttempt],
		tar.[ReceiverCity] = sour.[ReceiverCity],
		tar.[inbounded_time_for_return] = sour.[inbounded_time_for_return],
		tar.[outbounded_time_for_return] = sour.[outbounded_time_for_return],
		tar.[pickupfailed_datetime] = sour.[pickupfailed_datetime],
		tar.[Pickup_pre_last_failattempt_datetime] = sour.[Pickup_pre_last_failattempt_datetime],
		tar.[ReasonPickUpUnsuccessful] = sour.[ReasonPickUpUnsuccessful],
		tar.[store_name] = sour.[store_name],
		tar.[Returned_datetime] = sour.[Returned_datetime],
		tar.[Canceled_datetime] = sour.[Canceled_datetime],
		tar.[Compensation_datetime] = sour.[Compensation_datetime],
		tar.[PU_Inbound_DateTime] = sour.[PU_Inbound_DateTime],
		tar.[PU_Inbound_Note] = sour.[PU_Inbound_Note],
		tar.[ShipmentId] = sour.[ShipmentId],
		tar.[Pickup_Store] = sour.[Pickup_Store],
		tar.[Delivery_Store] = sour.[Delivery_Store],
		tar.[Last_updated_datetime] = sour.[Last_updated_datetime],
		tar.[sr_state] = sour.[sr_state],
		tar.[sr_status] = sour.[sr_status],
		tar.[closed_time] = sour.[closed_time],
		tar.[shipmentrequestid] = sour.[shipmentrequestid],
		tar.[send_handing_over_time] = sour.[send_handing_over_time],
		tar.[send_handed_over_time] = sour.[send_handed_over_time],
		tar.[start_returning_time] = sour.[start_returning_time],
		tar.[completed_returned_time] = sour.[completed_returned_time],
		tar.[completed_delivery_time] = sour.[completed_delivery_time],
		tar.[start_delivery_time] = sour.[start_delivery_time],
		tar.[second_attemp_delivery_time] = sour.[second_attemp_delivery_time],
		tar.[max_handing_over_time] = sour.[max_handing_over_time],
		tar.[start_transfer_time_to_return] = sour.[start_transfer_time_to_return],
		tar.[end_transfer_time_to_return] = sour.[end_transfer_time_to_return],
		tar.[inbounded_time_sc_for_return] = sour.[inbounded_time_sc_for_return],
		tar.[outbound_time_sc_for_return] = sour.[outbound_time_sc_for_return],
		tar.[inbounded_sc_time_for_send] = sour.[inbounded_sc_time_for_send],
		tar.[outbounded_time_sc_for_send] = sour.[outbounded_time_sc_for_send],
		tar.[inbounded_time_for_send] = sour.[inbounded_time_for_send],
		tar.[expected_pickup_time] = sour.[expected_pickup_time],
		tar.[expected_delivery_rod_time] = sour.[expected_delivery_rod_time],
		tar.[expected_transfer_time_for_send] = sour.[expected_transfer_time_for_send],
		tar.[expected_transfer_time_to_return] = sour.[expected_transfer_time_to_return],
		tar.[expected_delivery_time] = sour.[expected_delivery_time],
		tar.[sku_code] = sour.[sku_code],
		tar.[updated_status] = sour.[updated_status],
		tar.[PackageQuantity] = sour.[PackageQuantity],
		tar.[PackageDescription] = sour.[PackageDescription],
		tar.[SenderPrimaryPhone] = sour.[SenderPrimaryPhone],
		tar.[ReceiverPrimaryPhone] = sour.[ReceiverPrimaryPhone],
		tar.[ReceiverName] = sour.[ReceiverName],
		tar.[partner_3pl] = sour.[partner_3pl],
		tar.[pickup_city_3pl] = sour.[pickup_city_3pl],
		tar.[delivery_city_3pl] = sour.[delivery_city_3pl],
		tar.[ordercode_3pl] = sour.[ordercode_3pl],
		tar.[Is3Pls] = sour.[Is3Pls],
		tar.[assign_datetime_3pl] = sour.[assign_datetime_3pl],
		tar.[pickuped_datetime_3pl] = sour.[pickuped_datetime_3pl],
		tar.[returned_datetime_3pl] = sour.[returned_datetime_3pl],
		tar.[raw_status_3pl] = sour.[raw_status_3pl],
		tar.[status_3pl] = sour.[status_3pl],
		tar.sender_id = sour.SenderId,
		tar.partner_id = sour.PartnerId


WHEN NOT MATCHED THEN
	INSERT (
		[shipper_name],
		[PartnerName],
		[City],
		[ma_package_khach_hang],
		[ma_package_],
		[partner_request_datetime],
		[ngay_gan_don],
		[delivery_fail_datetime],
		[delivery_fail_reason],
		[delivered_datetime],
		[createdtrip_datetime],
		[assigntrip_datetime],
		[status],
		[sm_table_state],
		[sm_table_status],
		[IsReturnable],
		[PickupDistrict],
		[PickupWard],
		[PickupRawAddress],
		[ReceiverDistrict],
		[ReceiverRawAddress],
		[ReceiverWard],
		[service_express],
		[ShippedTime],
		[COD],
		[Weight],
		[SenderName],
		[Picked_up_datetime],
		[Picked_up_Shipper],
		[Pickup_unsuccess_datetime],
		[Pickup_last_failattempt_datetime],
		[NoDeliveryAttempt],
		[NoPickupAttempt],
		[ReceiverCity],
		[inbounded_time_for_return],
		[outbounded_time_for_return],
		[pickupfailed_datetime],
		[Pickup_pre_last_failattempt_datetime],
		[ReasonPickUpUnsuccessful],
		[store_name],
		[Returned_datetime],
		[Canceled_datetime],
		[Compensation_datetime],
		[PU_Inbound_DateTime],
		[PU_Inbound_Note],
		[ShipmentId],
		[Pickup_Store],
		[Delivery_Store],
		[Last_updated_datetime],
		[sr_state],
		[sr_status],
		[closed_time],
		[shipmentrequestid],
		[send_handing_over_time],
		[send_handed_over_time],
		[start_returning_time],
		[completed_returned_time],
		[completed_delivery_time],
		[start_delivery_time],
		[second_attemp_delivery_time],
		[max_handing_over_time],
		[start_transfer_time_to_return],
		[end_transfer_time_to_return],
		[inbounded_time_sc_for_return],
		[outbound_time_sc_for_return],
		[inbounded_sc_time_for_send],
		[outbounded_time_sc_for_send],
		[inbounded_time_for_send],
		[expected_pickup_time],
		[expected_delivery_rod_time],
		[expected_transfer_time_for_send],
		[expected_transfer_time_to_return],
		[expected_delivery_time],
		[sku_code],
		[updated_status],
		[PackageQuantity],
		[PackageDescription],
		[SenderPrimaryPhone],
		[ReceiverPrimaryPhone],
		[ReceiverName],
		[partner_3pl],
		[pickup_city_3pl],
		[delivery_city_3pl],
		[ordercode_3pl],
		[Is3Pls],
		[assign_datetime_3pl],
		[pickuped_datetime_3pl],
		[returned_datetime_3pl],
		[raw_status_3pl],
		[status_3pl],
		sender_id,
		partner_id
)

VALUES (
		sour.[shipper_name],
		sour.[PartnerName],
		sour.[City],
		sour.[ma_package_khach_hang],
		sour.[ma_package_],
		sour.[partner_request_datetime],
		sour.[ngay_gan_don],
		sour.[delivery_fail_datetime],
		sour.[delivery_fail_reason],
		sour.[delivered_datetime],
		sour.[createdtrip_datetime],
		sour.[assigntrip_datetime],
		sour.[status],
		sour.[sm_table_state],
		sour.[sm_table_status],
		sour.[IsReturnable],
		sour.[PickupDistrict],
		sour.[PickupWard],
		sour.[PickupRawAddress],
		sour.[ReceiverDistrict],
		sour.[ReceiverRawAddress],
		sour.[ReceiverWard],
		sour.[service_express],
		sour.[ShippedTime],
		sour.[COD],
		sour.[Weight],
		sour.[SenderName],
		sour.[Picked_up_datetime],
		sour.[Picked_up_Shipper],
		sour.[Pickup_unsuccess_datetime],
		sour.[Pickup_last_failattempt_datetime],
		sour.[NoDeliveryAttempt],
		sour.[NoPickupAttempt],
		sour.[ReceiverCity],
		sour.[inbounded_time_for_return],
		sour.[outbounded_time_for_return],
		sour.[pickupfailed_datetime],
		sour.[Pickup_pre_last_failattempt_datetime],
		sour.[ReasonPickUpUnsuccessful],
		sour.[store_name],
		sour.[Returned_datetime],
		sour.[Canceled_datetime],
		sour.[Compensation_datetime],
		sour.[PU_Inbound_DateTime],
		sour.[PU_Inbound_Note],
		sour.[ShipmentId],
		sour.[Pickup_Store],
		sour.[Delivery_Store],
		sour.[Last_updated_datetime],
		sour.[sr_state],
		sour.[sr_status],
		sour.[closed_time],
		sour.[shipmentrequestid],
		sour.[send_handing_over_time],
		sour.[send_handed_over_time],
		sour.[start_returning_time],
		sour.[completed_returned_time],
		sour.[completed_delivery_time],
		sour.[start_delivery_time],
		sour.[second_attemp_delivery_time],
		sour.[max_handing_over_time],
		sour.[start_transfer_time_to_return],
		sour.[end_transfer_time_to_return],
		sour.[inbounded_time_sc_for_return],
		sour.[outbound_time_sc_for_return],
		sour.[inbounded_sc_time_for_send],
		sour.[outbounded_time_sc_for_send],
		sour.[inbounded_time_for_send],
		sour.[expected_pickup_time],
		sour.[expected_delivery_rod_time],
		sour.[expected_transfer_time_for_send],
		sour.[expected_transfer_time_to_return],
		sour.[expected_delivery_time],
		sour.[sku_code],
		sour.[updated_status],
		sour.[PackageQuantity],
		sour.[PackageDescription],
		sour.[SenderPrimaryPhone],
		sour.[ReceiverPrimaryPhone],
		sour.[ReceiverName],
		sour.[partner_3pl],
		sour.[pickup_city_3pl],
		sour.[delivery_city_3pl],
		sour.[ordercode_3pl],
		sour.[Is3Pls],
		sour.[assign_datetime_3pl],
		sour.[pickuped_datetime_3pl],
		sour.[returned_datetime_3pl],
		sour.[raw_status_3pl],
		sour.[status_3pl],
		senderid,
		PartnerId
		
);

SET ANSI_WARNINGS ON;
END