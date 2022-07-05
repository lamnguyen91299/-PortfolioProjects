USE [ship60databaseRepl]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[Ship60_Performance_LastStatus_v8_NHLAM]
AS

BEGIN
With 

tracking_shipment_assign_3pl as (
	SELECT
		s.ordercode ,
		ts.*,
		ROW_NUMBER() OVER(PARTITION BY ts.status,s.ordercode Order by s.ordercode DESC) as rn
	FROM TrackingShipments ts 
	LEFT JOIN shipments s on ts.shipmentid = s.id
	WHERE 
			(ts.Status = 'Active' or ts.Status = '3pls_picking') and
			ts.Is3Pls = '1' and
			ts.createdtime >=DATEADD(DAY, -60, GETDATE())
),
tracking_shipment_pickup_3pl as (
	SELECT
		s.ordercode ,
		ts.*,
		ROW_NUMBER() OVER(PARTITION BY ts.status,s.ordercode Order by s.ordercode DESC) as rn
	FROM TrackingShipments ts 
	LEFT JOIN shipments s on ts.shipmentid = s.id
	WHERE  
		ts.Status = 'Pickuped' and
		ts.Is3Pls = '1' and
		ts.createdtime >=DATEADD(DAY, -60, GETDATE())
),

tbl as
(
SELECT
	u.Name as 'shipper_name'
	, sr.CustomerOrderCode as 'ma_package_khach_hang'
	,case 			
		when sr.Status = 'Canceled' then N'01. Huỷ bởi đối tác'
		when sr.State = 'compensation' then N'15. Chốt đền bù'
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
		when s.status = 'Pickuped' then N'05. Thu gom TC'
		when s.status = 'CanceledPickup' then N'04. Đang thu gom - lại'
		when s.status = 'PickupUnsuccessful' and s.State = 'PickupFailed' and sr.PickupShipmentCreated = 1 then N'07. Thu gom KTC - huỷ (FINAL)'
		when s.status = 'PickupUnsuccessful' and (s.ShipmentType = 'Pickup' OR s.ShipmentType IS NULL) then N'06. Thu gom KTC'				
		when (sr.status = 'New' AND s.Status IS NULL AND sr.IsShipped = 0) 
		OR (sr.status = 'AddressUpdated' and sr.PickupShipmentCreated = 0) 
		then N'03. Chưa thu gom'
		when sr.ServiceType = 'FMRT' and sr.status = 'AddressUpdated' and sr.IsShipped = 0 then N'14. Yêu cầu chuyển hoàn mới'				
		when sr.ServiceType = 'FMRT' and sr.status = 'AddressUpdated' and sr.IsShipped = 1 then N'14. Nhập kho chờ hoàn'				
		when sr.status = 'AddressUpdated' then N'08. Nhập kho chờ giao - hub'				
		when s.status = 'Canceled' then N'02. Huỷ bởi Ship60'
		else N'CHƯA THÔNG BÁO RIDER' 
	end as 'status'
	, s.reasonsendunsuccess
	, s.ReasonPickupUnsuccessful
	, sr.createdtime as 'partner_request_datetime'
	, ts5.createdtime as 'complete_datetime'
	, ts13.createdtime as 'Return_datetime'
	, sr.cod as 'COD'
	, sr.PickupDistrict as 'from_district'
	, sr.receiverdistrict as 'to_district'
	, sr.ReceiverWard as 'to_ward'
	, sr.PickupRawAddress as 'from'
	, sr.ReceiverRawAddress as 'to'
	, gr.Partner
	, sr.PickupCity as 'City'
	,CASE WHEN s.OrderCode IS NOT NULL THEN ROW_NUMBER() OVER (PARTITION BY s.OrderCode ORDER BY  s.Id DESC) ELSE NULL END as RowNo
	, sr.ordercode as 'ma_package_ship60'
	, sr.ReceiverPrimaryPhone
	, sr.ReceiverName
	--, sr.StoreId
	, gr.SenderPrimaryPhone
	, ts7.CreatedTime as 'Picked_up_datetime'
	, u.Name as 'rider_return'
	, COALESCE(u3.name,gr.SenderName) as 'SenderName'
	, COALESCE(u2.ownerid,gr.SenderId) as Senderid
	, sr.ShippedTime
	, sr.ServiceType
	, sr.Weight
	, COALESCE(sr.ValueManifest,sr.ValueGoods) as 'ValueGoods'
	, ps.Name as StoreName
	, sr.IsReturnable
	, sr.NoDeliveryAttempt
	, sr.NoReturnAttempt
	, sr.PackageDescription
	, sr.NoPickupAttempt
	, sr.PickupCity
	, sr.ReceiverCity
	, sr.PickupWard
	, case
		when s.ModifiedTime is null then sr.ModifiedTime
		when sr.ModifiedTime > s.ModifiedTime then sr.ModifiedTime
		else s.ModifiedTime
	end	as 'Last_updated_datetime'
	, case when ts10.Status = 'handing_over' then 1 else 0 end as 'Is_on_transit'
	, sr.PackageQuantity
	, iif (sr.NeedToReDelivery = 1, N'YES', 'NO') as 'NeedToReDelivery'
	, CASE
		WHEN ts14.Is3Pls = 1 and lower(u.name) LIKE '%ghn%' THEN 'GHN'
		WHEN ts14.Is3Pls = 1 and lower(u.name) LIKE '%ninjavan%' THEN 'NJV'
		WHEN ts14.is3Pls = 1 THEN u.name
		ELSE NULL 
	END as '3pl_partner'
	,CASE WHEN ts14.Is3Pls = 1 THEN ps.city ELSE NULL END as '3pl_pickup_city'
	,CASE WHEN ts14.Is3Pls = 1 THEN sr.ReceiverCity ELSE NULL END as '3pl_delivery_city'
	,s.id as shipmentids
	,s.TrackingNumberId as '3pl_ordercode'
	,ts14.Is3Pls
	,ts14.CreatedTime as '3pl_assign_datetime'
	,ts15.CreatedTime as '3pl_pickuped_datetime'
from GroupShipmentRequests gr 
	left join shipmentrequests sr 
		on sr.GroupShipmentId = gr.Id
	left join shipments s 
		on sr.Id = s.ShipmentRequestId and s.DataStatus != 2
	left join shipments s2 
		on sr.Id = s2.ShipmentRequestId and s2.ShipmentType = 'Pickup' and s2.Status = 'Pickuped' and s2.DataStatus != 2 
	left join shipmentusers su 
		on su.groupshipmentid = s.GroupShipmentId and su.isselected = 1
	left join users u 
		on su.userid = u.id	  	
	left join trackingshipments ts5 
		on  ts5.id = (select min(id) from trackingshipments 
		where [status] = 'Completed' and shipperid = su.userid and shipmentid = s.id and State is null)
	left join TrackingShipments ts7 on ts7.Id = 
		(Select min(id) from TrackingShipments where ShipmentId = s2.Id and Status = 'Pickuped') 
	left join PartnerStores ps on sr.StoreId = ps.Id	
	left join TrackingShipments ts10 on ts10.Id = 
		(Select max(id) from TrackingShipments where ShipmentRequestid = sr.Id) 
	left join TrackingShipments ts11 on ts11.Id = 
		(Select min(id) from TrackingShipments where ShipmentId = s2.Id and state = 'StorageToDelivery') 
	left join trackingshipments ts12 on s.id = ts12.shipmentid 
		and ts12.id = (select max(id) from trackingshipments 
		where [status] = 'Completed' and s.id = shipmentid and [state] = 'IsPaidCODToCustomer')
	left join trackingshipments ts13 on  ts13.id = (select min(id) from trackingshipments 
		where [status] = 'Returned' and shipperid = su.userid and shipmentid = s.id and State is null)
	left join (SELECT * FROM tracking_shipment_assign_3pl WHERE rn = 1) ts14 on ts14.OrderCode = sr.ordercode
	left join (SELECT * FROM tracking_shipment_pickup_3pl WHERE rn = 1) ts15 on ts15.OrderCode = sr.ordercode
	left join users as u2 on u2.id = gr.SenderId
	left join users as u3 on u2.OwnerId = u3.id


where	
	(gr.DataStatus is null or gr.DataStatus != 2) 
	and (sr.DataStatus is null or sr.DataStatus != 2)
	and (s.DataStatus is null or s.DataStatus != 2)	    
	and (sr.createdtime >=  DATEADD(DAY, -60, GETDATE())) 
	 --and (sr.createdtime >=  @Starttime) 
)

SELECT *
FROM tbl
where tbl.RowNo = 1 or tbl.RowNo is null

END
