__author__ = 'Stiander'

import grpc
import libs.qrpcclientlib.vedbjorn_pb2 as vedbjorn_pb2
import libs.qrpcclientlib.vedbjorn_pb2_grpc as vedbjorn_pb2_grpc
import os
import google.oauth2.id_token

QRPC_HOST = os.getenv('QRPC_HOST' , 'qrpcapi')
QRPC_PORT = os.getenv('QRPC_PORT' , 50051)

#AUDIENCE = 'https://vedbjorn-grpc-server-36aidthlda-lz.a.run.app'
AUDIENCE = 'https://' + QRPC_HOST

access_token = google.oauth2.id_token.fetch_id_token(google.auth.transport.requests.Request(), AUDIENCE)
host = QRPC_HOST + ':' + str(QRPC_PORT)

IS_FAKE = os.getenv('DEBUGGING' , '') == 'true'

cred = None
if IS_FAKE :
    with open('./certificate_local.pem', 'rb') as f:
        certificate = f.read()
        cred = grpc.ssl_channel_credentials(certificate)
else :
    cred = grpc.ssl_channel_credentials()

def GetMarketInfo(locationObj : dict) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        loc: vedbjorn_pb2.Location = dict_to_Location(locationObj)
        response = stub.GetMarketInfo(loc, metadata=[('authorization', 'Bearer ' + access_token)])
        if response.info.ok == True:
            return {
                'num_sellers': response.num_sellers,
                'num_buyers': response.num_buyers,
                'num_drivers': response.num_drivers
            }
        else:
            return {}


def hard_bool(_obj, _keyword : str = '') -> bool :
    if isinstance(_obj, bool) :
        return _obj
    if _keyword == '' :
        try :
            conv : bool = bool(_obj)
            return conv
        except Exception :
            return False
    if not _obj or not _keyword:
        return False
    _lowkey = _keyword.lower()
    _midkey = str(_keyword[0]).upper() + _lowkey[1:len(_lowkey)]
    _topkey = _keyword.upper()
    fake_val = _obj.get(_lowkey , _obj.get(_midkey, _obj.get(_topkey, False)))
    if isinstance(fake_val, bool) :
        return fake_val
    elif isinstance(fake_val, str) :
        return fake_val.lower() == 'true'
    else :
        try :
            return bool(fake_val)
        except Exception :
            return False

def Location_to_dict(loc : vedbjorn_pb2.Location) -> dict:
    return {
        'lat': loc.lat,
        'lng': loc.lng,
        'place_id': loc.place_id,
        'osm_type': loc.osm_type,
        'osm_id' : loc.osm_id ,
        'class' : loc.lclass ,
        'type' : loc.ltype ,
        'importance' : loc.importance,
        'display_name': loc.display_name,
        'road': loc.road,
        'quarter': loc.quarter,
        'village': loc.village,
        'farm': loc.farm,
        'municipality': loc.municipality,
        'county': loc.county,
        'country': loc.country,
        'postcode': loc.postcode,
        'name': loc.name,
        'info' : {
            'content' : loc.info.content ,
            'code' : loc.info.code ,
            'ok' : loc.info.ok
        }
    }

def dict_to_Location(loc : dict) -> vedbjorn_pb2.Location:
    return vedbjorn_pb2.Location(
        lat          = float(loc.get('lat' , -1)),
        lng          = float(loc.get('lng', -1)),
        place_id     = int(loc.get('place_id', -1)),
        osm_type     = str(loc.get('osm_type', '')),
        display_name = str(loc.get('display_name', '')),
        road         = str(loc.get('road', '')),
        quarter      = str(loc.get('quarter', '')),
        village      = str(loc.get('village' , '')),
        farm         = str(loc.get('farm', '')),
        municipality = str(loc.get('municipality', '')),
        county       = str(loc.get('county', '')),
        country      = str(loc.get('country', '')),
        postcode     = str(loc.get('postcode', '')),
        osm_id       = int(loc.get('osm_id' , -1)) ,
        lclass       = str(loc.get('class' , '')),
        ltype        = str(loc.get('type' , '')),
        importance   = float(loc.get('importance' , -1)) ,
        name         = str(loc.get('name', '')) ,
        info = vedbjorn_pb2.GeneralInfoMessage(
            content = str(loc.get('info' , {}).get('content' , '')) ,
            code    = int(loc.get('info' , {}).get('code' , 0)) ,
            ok      = hard_bool(loc.get('info' , {}).get('ok' , True))
        )
    )

def BuyRequest_to_dict(buyreq : vedbjorn_pb2.BuyRequest) -> dict :
    if buyreq.contact_info.email_address == '' :
        return {
            'content' : buyreq.info.content ,
            'code' : buyreq.info.code ,
            'ok' : buyreq.info.ok
        }
    return {
        'email' : buyreq.contact_info.email_address ,
        'phone': buyreq.contact_info.phone_number,
        'name' : buyreq.name ,
        'current_requirement' : buyreq.current_requirement,
        'reserved_weeks': buyreq.reserved_weeks,
        'reserve_target': buyreq.reserve_target,
        'last_calced': buyreq.last_calced,
        'claimed_by_driver': buyreq.claimed_by_driver,
        'fake': buyreq.fake ,
        'info' : {
            'content' : buyreq.info.content ,
            'code' : buyreq.info.code ,
            'ok' : buyreq.info.code
        }
    }

def SellRequest_to_dict(sellreq : vedbjorn_pb2.SellRequest) -> dict :
    if sellreq.contact_info.email_address == '' :
        return {
            'content' : sellreq.info.content ,
            'code' : sellreq.info.code ,
            'ok' : sellreq.info.ok
        }
    return {
        'email' : sellreq.contact_info.email_address ,
        'phone': sellreq.contact_info.phone_number,
        'name' : sellreq.name ,
        'current_capacity' : sellreq.current_capacity ,
        'amount_reserved' : sellreq.amount_reserved ,
        'amount_staged' : sellreq.amount_staged ,
        'num_reserved' : sellreq.num_reserved ,
        'num_staged' : sellreq.num_staged ,
        'prepare_for_pickup' : sellreq.prepare_for_pickup ,
        'fake' : sellreq.fake ,
        'info' : {
            'content' : sellreq.info.content ,
            'code' : sellreq.info.code ,
            'ok' : sellreq.info.code
        }
    }

def DriveRequest_to_dict(drivereq : vedbjorn_pb2.DriveRequest) -> dict :
    return {
        'email': drivereq.contact_info.email_address,
        'phone': drivereq.contact_info.phone_number,
        'name' : drivereq.name ,
        'available' : drivereq.available ,
        'num_staged_pickups' : drivereq.num_staged_pickups ,
        'fake' : drivereq.fake ,
        'info': {
            'content': drivereq.info.content,
            'code': drivereq.info.code,
            'ok': drivereq.info.code
        }
    }

def User_to_dict(user : vedbjorn_pb2.User) -> dict :
    return {
        'firstname' : user.firstname ,
        'lastname' : user.lastname ,
        'email' : user.contact_info.email_address ,
        'phone': user.contact_info.phone_number,
        'location_name' : user.location_name ,
        'fake' : user.fake ,
        'info' : {
            'content' : user.info.content ,
            'code' : user.info.code ,
            'ok' : user.info.ok
        }
    }

def Routes_to_dict(routes : vedbjorn_pb2.Routes) -> dict :
    if routes.info.code != 200 :
        return {'info' : {
                    'content' : routes.info.content ,
                    'code' : routes.info.code ,
                    'ok' : False
                }}
    ret : dict = {
        'id' : routes.id ,
        'driveRequestName' : routes.driveRequestName ,
        'status' : routes.status ,
        'created_UTC' : routes.created_UTC ,
        'calc_time' : routes.calc_time ,
        'updated' : routes.updated ,
        'accept_deadline' : routes.accept_deadline ,
        'route' : [] ,
        'deals' : {} ,
        'due' : routes.due ,
        'wrapup' : routes.wrapup ,
        'finished_time' : routes.finished_time ,
        'finished_time_str' : routes.finished_time_str ,
        'info' : {
            'content' : routes.info.content ,
            'code' : routes.info.code ,
            'ok' : routes.info.ok
        }
    }
    for visit in routes.route :
        visit_dict = {
            'from' : Location_to_dict(visit.from_loc) ,
            'to' : Location_to_dict(visit.to_loc) ,
            'distance' : visit.distance ,
            'type' : visit.type ,
            'loaded_before' : visit.loaded_before ,
            'loaded_after' : visit.loaded_after ,
            'sellRequest' : SellRequest_to_dict(visit.sellRequest) ,
            'driveRequest' : DriveRequest_to_dict(visit.driveRequest)  ,
            'drive_user' : User_to_dict(visit.drive_user) ,
            'visited' : visit.visited ,
            'visited_status' : visit.visited_status ,
            'status' : visit.status ,
            'return_amount' : visit.return_amount ,
            'notification' : visit.notification
        }
        if visit.buyRequest.name != '' :
            visit_dict['buyRequest'] = BuyRequest_to_dict(visit.buyRequest)
        ret['route'].append(visit_dict)
    for deal in routes.deals :
        deal_dict = {
            'sellRequest' : SellRequest_to_dict(deal.sellRequest) ,
            'number_of_bags_sold' : deal.number_of_bags_sold ,
            'sells' : []
        }
        for sell in deal.sells :
            deal_dict['sells'].append({
                'reserved_weeks' : sell.reserved_weeks ,
                'last_calced' : sell.last_calced ,
                'claimed_by_driver' : sell.claimed_by_driver ,
                'reserve_target' : sell.reserve_target ,
                'current_requirement' : sell.current_requirement ,
                'name' : sell.name ,
                'fake' : sell.fake
            })
        ret['deals'][deal.sellName] = deal_dict
    return ret

def CoordinateToLocation(lat : float , lng : float):
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.CoordinateToLocation(vedbjorn_pb2.Coordinate(lat=lat, lng=lng),
                                             metadata=[('authorization', 'Bearer ' + access_token)])
        return Location_to_dict(response)

def NameToLocation(name : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.NameToLocation(vedbjorn_pb2.GeneralInfoMessage(
            content = name
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return Location_to_dict(response)

def FindCoordinatesInAddress(locationObj : dict) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        loc : vedbjorn_pb2.Location = dict_to_Location(locationObj)
        response = stub.FindCoordinatesInAddress(loc, metadata=[('authorization', 'Bearer ' + access_token)])
        return Location_to_dict(response)

def LocationToGraph(lat : float , lng : float):
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.LocationToGraph(vedbjorn_pb2.Coordinate(lat=lat, lng=lng),
                                        metadata=[('authorization', 'Bearer ' + access_token)])
        return Location_to_dict(response)

def Message_to_dict(msg : vedbjorn_pb2.Message) -> dict :
    return {
        'timestamp' : msg.timestamp ,
        'email' : msg.email ,
        'emailSender' : msg.emailSender ,
        'contentType' : msg.contentType ,
        'amount' : msg.amount ,
        'ref_collection' : msg.ref_collection ,
        'ref_id' : msg.ref_id ,
        'text' : msg.text ,
        'status' : msg.status ,
        'info' : {
            'content' : msg.info.content ,
            'code' : msg.info.code ,
            'ok' : msg.info.ok
        }
    }

def GetUser(email : str = '' , phone : str = '') :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetUser(vedbjorn_pb2.UserContactInfo(
            email_address = email ,
            phone_number = phone
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'location_name' : response.location_name ,
            'firstname' : response.firstname ,
            'lastname' : response.lastname ,
            'email' : response.contact_info.email_address ,
            'phone' : response.contact_info.phone_number ,
            'fake' : response.fake,
            'info': {
                'content': response.info.content,
                'code': response.info.code,
                'ok': response.info.ok
            } ,
            'is_admin' : response.is_admin
        }

def CreateUser(user : dict) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.CreateUser(vedbjorn_pb2.User(
            contact_info=vedbjorn_pb2.UserContactInfo(
                email_address = user['email'] ,
                phone_number = user['phone']
            ) ,
            firstname=user['firstname'] ,
            lastname=user['lastname'] ,
            location_name=user['location_name']
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content' : response.content ,
            'code' : response.code ,
            'ok' : response.ok
        }

def DeleteUser(email : str, phone : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.DeleteUser(vedbjorn_pb2.UserContactInfo(
            email_address = email ,
            phone_number = phone
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content': response.content,
            'code': response.code,
            'ok': response.ok
        }

def BuyRequestToUser(email : str , buyreq : dict) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.BuyRequestToUser(vedbjorn_pb2.BuyRequest(
            contact_info = vedbjorn_pb2.UserContactInfo(
                email_address = email
            ) ,
            current_requirement = buyreq['current_requirement'] ,
            reserved_weeks = buyreq['reserved_weeks'] ,
            reserve_target = '' ,
            last_calced = 0 ,
            claimed_by_driver = False ,
            fake = IS_FAKE
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content': response.content,
            'code': response.code,
            'ok': response.ok
        }

def GetBuyRequest(email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetBuyRequest(vedbjorn_pb2.UserContactInfo(email_address = email),
                                      metadata=[('authorization', 'Bearer ' + access_token)])
        return BuyRequest_to_dict(response)

def GetBuyRequestMatch(email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetBuyRequestMatch(vedbjorn_pb2.UserContactInfo(email_address = email),
                                           metadata=[('authorization', 'Bearer ' + access_token)])
        return response.content

def DeleteBuyRequest(email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.DeleteBuyRequest(vedbjorn_pb2.UserContactInfo(email_address = email),
                                         metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content': response.content,
            'code': response.code,
            'ok': response.ok
        }

def VerifyUserEmailStart(email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.VerifyUserEmailStart(vedbjorn_pb2.UserContactInfo(email_address=email),
                                             metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content': response.content,
            'code': response.code,
            'ok': response.ok
        }

def VerifyUserEmail(email : str, code : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.VerifyUserEmail(vedbjorn_pb2.EmailVerificationCode(
            email_address=email ,
            code=code
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content': response.content,
            'code': response.code,
            'ok': response.ok
        }

def SellRequestToUser(email : str , sellreq : dict) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.SellRequestToUser(vedbjorn_pb2.SellRequest(
            contact_info = vedbjorn_pb2.UserContactInfo(
                email_address = email
            ),
            current_capacity=sellreq['current_capacity'],
            amount_reserved=0,
            amount_staged=0,
            num_reserved=0,
            num_staged=0,
            prepare_for_pickup=0,
            fake=IS_FAKE
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content': response.content,
            'code': response.code,
            'ok': response.ok
        }

def GetSellRequest(email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetSellRequest(vedbjorn_pb2.UserContactInfo(email_address = email),
                                       metadata=[('authorization', 'Bearer ' + access_token)])
        return SellRequest_to_dict(response)

def DeleteSellRequest(email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.DeleteSellRequest(vedbjorn_pb2.UserContactInfo(email_address = email),
                                          metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content': response.content,
            'code': response.code,
            'ok': response.ok
        }

def DriveRequestToUser(email : str , drivereq : dict) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.DriveRequestToUser(vedbjorn_pb2.DriveRequest(
            contact_info=vedbjorn_pb2.UserContactInfo(
                email_address=email
            ),
            available=drivereq['available'],
            num_staged_pickups=0,
            fake=IS_FAKE
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content': response.content,
            'code': response.code,
            'ok': response.ok
        }

def GetDriveRequest(email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetDriveRequest(vedbjorn_pb2.UserContactInfo(email_address = email),
                                        metadata=[('authorization', 'Bearer ' + access_token)])
        return DriveRequest_to_dict(response)

def DeleteDriveRequest(email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.DeleteDriveRequest(vedbjorn_pb2.UserContactInfo(email_address = email),
                                           metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content': response.content,
            'code': response.code,
            'ok': response.ok
        }

def ShortCountryInfo(country : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.ShortCountryInfo(vedbjorn_pb2.Name(value = country),
                                         metadata=[('authorization', 'Bearer ' + access_token)])
        ret : dict = {}
        for cnti in response.counties :
            cntiName = cnti.name.value
            ret[cntiName] = []
            for muni in cnti.municipalities :
                ret[cntiName].append(muni)
        return ret

def SetDriverNotAvailable(email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.SetDriverNotAvailable(vedbjorn_pb2.UserContactInfo(email_address = email),
                                              metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content': response.content,
            'code': response.code,
            'ok': response.ok
        }

def GetPlannedRoute(name : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetPlannedRoute(vedbjorn_pb2.Name(value = name),
                                        metadata=[('authorization', 'Bearer ' + access_token)])
        return Routes_to_dict(response)

def GetOngoingRoute(name : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetOngoingRoute(vedbjorn_pb2.Name(value = name),
                                        metadata=[('authorization', 'Bearer ' + access_token)])
        return Routes_to_dict(response)

def GetCompletedRoutes(name : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetCompletedRoutes(vedbjorn_pb2.Name(value = name),
                                           metadata=[('authorization', 'Bearer ' + access_token)])
        ret : list = []
        for rt in response.routesList :
            ret.append(Routes_to_dict(rt))
        return ret

def SetAcceptPlannedRoute(name : str, accepted : bool) :
    with grpc.secure_channel(host, cred) as channel:
        _codeint : int = 0
        if accepted :
            _codeint = 1
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.SetAcceptPlannedRoute(vedbjorn_pb2.GeneralInfoMessage(
            content = name ,
            code = _codeint
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content': response.content,
            'code': response.code,
            'ok': response.ok
        }

def NotifyDriverOnNewMission(driverName : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.NotifyDriverOnNewMission(vedbjorn_pb2.Name(value = driverName),
                                                 metadata=[('authorization', 'Bearer ' + access_token)])

        return {
            'content': response.content,
            'code': response.code,
            'ok': response.ok
        }

def PushVisit(visitInfo : dict) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.PushVisit(vedbjorn_pb2.VisitProof(
            img = visitInfo['img'] ,
            visitIndex = visitInfo['index'] ,
            driverName = visitInfo['driverName'] ,
            type = visitInfo['type'] ,
            img_text = visitInfo['img_text'] ,
            timestamp = visitInfo['timestamp']
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content': response.content,
            'code': response.code,
            'ok': response.ok
        }

def GetVisit(index : int , driverName : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetVisit(vedbjorn_pb2.VisitIndex(
            index = index ,
            driverName = driverName
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'img' : response.img ,
            'visitIndex' : response.visitIndex ,
            'driverName' : response.driverName ,
            'type' : response.type ,
            'img_text' : response.img_text ,
            'timestamp' : response.timestamp ,
            'info' : {
                'content' : response.info.content ,
                'code' : response.info.code ,
                'ok' : response.info.ok
            }
        }

def GetDeliveryProof(id : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetDeliveryProof(vedbjorn_pb2.Name(
            value = id
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'img' : response.img ,
            'img_text' : response.img_text ,
            'timestamp' : response.timestamp ,
            'info' : {
                'content' : response.info.content ,
                'code' : response.info.code ,
                'ok' : response.info.ok
            }
        }

def PushFeedbackComplaintNondelivery(email : str, ongoing_route : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.PushFeedbackComplaintNondelivery(vedbjorn_pb2.FeedbackComplaintNondelivery(
            buyerEmail = email ,
            ongoingRouteIt = ongoing_route
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content' : response.content ,
            'code' : response.code ,
            'ok' : response.ok
        }

def GetMessages(query : dict) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetMessages(vedbjorn_pb2.MessageQuery(
            receiverEmail = query.get('receiverEmail' , ''),
            senderEmail = query.get('senderEmail', ''),
            from_time = float(query.get('from_time', 0)),
            to_time = float(query.get('to_time', 0)),
            indices = query.get('indices', []),
            action = query.get('action', '')
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        msgs : list = []
        for msg in response.messages :
            msgs.append(Message_to_dict(msg))
        return msgs

def GetBuyRequestNotification(query : dict) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetBuyRequestNotification(vedbjorn_pb2.MessageQuery(
            receiverEmail = query.get('receiverEmail' , ''),
            senderEmail = query.get('senderEmail', ''),
            from_time = float(query.get('from_time', 0)),
            to_time = float(query.get('to_time', 0)),
            indices = query.get('indices', []),
            action = query.get('action', '')
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return Message_to_dict(response)

def PushFeedbackAcceptDelivery(email : str, notif_id : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.PushFeedbackAcceptDelivery(vedbjorn_pb2.MessageQuery(
            receiverEmail = email,
            action = notif_id
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content': response.content,
            'code': response.code,
            'ok': response.ok
        }

def PushFeedbackRejectDelivery(email : str, notif_id : str,wrongAmount : bool , wrongPrice : bool, wrongQuality : bool, customMessage : str = '') :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.PushFeedbackRejectDelivery(vedbjorn_pb2.FeedbackRejectDelivery(
            buyerEmail = email,
            notif_id = notif_id,
            wrongAmount = wrongAmount,
            wrongPrice = wrongPrice,
            wrongQuality = wrongQuality,
            customMessage = customMessage
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content': response.content,
            'code': response.code,
            'ok': response.ok
        }

def GetAllCompletedDeliveryInfoForBuyer(email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetAllCompletedDeliveryInfoForBuyer(vedbjorn_pb2.MessageQuery(
            receiverEmail=email
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        ret : list = []
        for delivery in response.deliveries :
            ret.append({
                'email' : delivery.email ,
                'time' : delivery.time ,
                'amount' : delivery.amount ,
                'driverEmail' : delivery.driverEmail ,
                'sellerEmail' : delivery.sellerEmail ,
                'status' : delivery.status ,
                'paidAmount' : delivery.paidAmount ,
                'notifications_id' : delivery.notifications_id ,
                'deliveries_id' : delivery.deliveries_id ,
                'info' : {
                    'content' : delivery.info.content ,
                    'code' : delivery.info.code ,
                    'ok' : delivery.info.ok
                }
            })
        return ret

def GetAllCompletedDeliveryInfoForBuyerAdm(email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetAllCompletedDeliveryInfoForBuyerAdm(vedbjorn_pb2.MessageQuery(
            receiverEmail=email
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        ret : list = []
        for delivery in response.deliveries :
            ret.append({
                'email' : delivery.email ,
                'time' : delivery.time ,
                'amount' : delivery.amount ,
                'driverEmail' : delivery.driverEmail ,
                'sellerEmail' : delivery.sellerEmail ,
                'status' : delivery.status ,
                'paidAmount' : delivery.paidAmount ,
                'notifications_id' : delivery.notifications_id ,
                'deliveries_id' : delivery.deliveries_id ,
                'info' : {
                    'content' : delivery.info.content ,
                    'code' : delivery.info.code ,
                    'ok' : delivery.info.ok
                }
            })
        return ret

def GetDeliveryReceipt(id : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetDeliveryReceipt(vedbjorn_pb2.DBQuery(
            object_id=id
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'bytes' : response.bytes,
            'media_type' : response.media_type ,
            'num_bytes' : response.num_bytes ,
            'info' : {
                'content' : response.info.content ,
                'code' : response.info.code,
                'ok' : response.info.ok
            }
        }

def GetDeliveryReceiptAdm(id : str , ismva : bool = True) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetDeliveryReceiptAdm(vedbjorn_pb2.DBQuery(
            object_id=id,
            attribute_name='mva',
            attribute_value=ismva
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'bytes' : response.bytes,
            'media_type' : response.media_type ,
            'num_bytes' : response.num_bytes ,
            'info' : {
                'content' : response.info.content ,
                'code' : response.info.code,
                'ok' : response.info.ok
            }
        }

def GetFinishedRouteReceipt(id : str, email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetFinishedRouteReceipt(vedbjorn_pb2.DBQuery(
            object_id=id ,
            attribute_name='email',
            attribute_value=email
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'bytes' : response.bytes,
            'media_type' : response.media_type ,
            'num_bytes' : response.num_bytes ,
            'info' : {
                'content' : response.info.content ,
                'code' : response.info.code,
                'ok' : response.info.ok
            }
        }

def GetFinishedRouteInvoice(id : str, email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetFinishedRouteInvoice(vedbjorn_pb2.DBQuery(
            object_id=id,
            attribute_name='email',
            attribute_value=email
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'bytes' : response.bytes,
            'media_type' : response.media_type ,
            'num_bytes' : response.num_bytes ,
            'info' : {
                'content' : response.info.content ,
                'code' : response.info.code,
                'ok' : response.info.ok
            }
        }

def GetNewSellerDealInfoList(email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetNewSellerDealInfoList(vedbjorn_pb2.UserContactInfo(
            email_address=email
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        ret : list = []
        for deal in response.deals :
            ret.append({
                'planned_routes_id' : deal.planned_routes_id ,
                'driverName' : deal.driverName ,
                'driverEmail' : deal.driverEmail ,
                'numBags' : deal.numBags ,
                'earningEstimate' : deal.earningEstimate ,
                'accepted' : deal.accepted ,
                'calc_time' : deal.calc_time
            })
        return ret

def GetOngoingSellerDealInfoList(email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetOngoingSellerDealInfoList(vedbjorn_pb2.UserContactInfo(
            email_address=email
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        ret : list = []
        for deal in response.deals :
            ret.append({
                'ongoing_routes_id' : deal.planned_routes_id ,
                'driverName' : deal.driverName ,
                'driverEmail' : deal.driverEmail ,
                'numBags' : deal.numBags ,
                'earningEstimate' : deal.earningEstimate ,
                'calc_time' : deal.calc_time
            })
        return ret

def GetNewSellerDealAccept(email : str, id : str, accept : bool) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetNewSellerDealAccept(vedbjorn_pb2.SellerDealAccept(
            planned_routes_id=id,
            sellerEmail=email,
            accept=accept
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'content': response.content,
            'code': response.code,
            'ok': response.ok
        }

def GetCompletedSells(email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        ret: list = []
        response = stub.GetCompletedSells(vedbjorn_pb2.UserContactInfo(
            email_address=email
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        for deal in response.deals:
            ret.append({
                'ongoing_routes_id': deal.planned_routes_id,
                'driverName': deal.driverName,
                'driverEmail': deal.driverEmail,
                'numBags': deal.numBags,
                'earningEstimate': deal.earningEstimate,
                'calc_time': deal.calc_time
            })
        return ret

def GetSellsReceipt(id : str, email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetSellsReceipt(vedbjorn_pb2.DBQuery(
            object_id=id ,
            attribute_name='email' ,
            attribute_value=email
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'bytes' : response.bytes,
            'media_type' : response.media_type ,
            'num_bytes' : response.num_bytes ,
            'info' : {
                'content' : response.info.content ,
                'code' : response.info.code,
                'ok' : response.info.ok
            }
        }

def GetSellsInvoice(id : str, email : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetSellsInvoice(vedbjorn_pb2.DBQuery(
            object_id=id ,
            attribute_name='email' ,
            attribute_value=email
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'bytes' : response.bytes,
            'media_type' : response.media_type ,
            'num_bytes' : response.num_bytes ,
            'info' : {
                'content' : response.info.content ,
                'code' : response.info.code,
                'ok' : response.info.ok
            }
        }

def GetPaymentInfo(notification_id : str, email : str = '') :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetPaymentInfo(vedbjorn_pb2.PaymentInfoQuery(
            notification_id=notification_id ,
            email=email
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'mongodb_id' : response.mongodb_id ,
            'paying_user_email' : response.paying_user_email ,
            'paying_user_phone' : response.paying_user_phone ,
            'receiving_user_name' : response.receiving_user_name ,
            'message_to_payer' : response.message_to_payer ,
            'ref_code' : response.ref_code ,
            'ref_collection' : response.ref_collection ,
            'ref_visit_id' : response.ref_visit_id ,
            'ref_route_id' : response.ref_route_id ,
            'status' : response.status ,
            'amount_NOK' : response.amount_NOK ,
            'calc_time' : response.calc_time ,
            'vipps_order_id' : response.vipps_order_id ,
            'info' : {
                'content' : response.info.content ,
                'code' : response.info.code ,
                'ok' : response.info.ok
            }
        }

def UpdatePaymentInfo_vippsOrderId(orderId : str, mongodb_id : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.UpdatePaymentInfo(vedbjorn_pb2.PaymentInfo(
            mongodb_id = mongodb_id ,
            vipps_order_id = orderId
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'mongodb_id' : response.mongodb_id ,
            'paying_user_email' : response.paying_user_email ,
            'paying_user_phone' : response.paying_user_phone ,
            'receiving_user_name' : response.receiving_user_name ,
            'message_to_payer' : response.message_to_payer ,
            'ref_code' : response.ref_code ,
            'ref_collection' : response.ref_collection ,
            'ref_visit_id' : response.ref_visit_id ,
            'ref_route_id' : response.ref_route_id ,
            'status' : response.status ,
            'amount_NOK' : response.amount_NOK ,
            'calc_time' : response.calc_time ,
            'vipps_order_id' : response.vipps_order_id ,
            'info' : {
                'content' : response.info.content ,
                'code' : response.info.code ,
                'ok' : response.info.ok
            }
        }

def UpdatePaymentInfo_paymentStatus(paymentStatus : str, mongodb_id : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.UpdatePaymentInfo(vedbjorn_pb2.PaymentInfo(
            mongodb_id = mongodb_id,
            status = paymentStatus
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'mongodb_id' : response.mongodb_id ,
            'paying_user_email' : response.paying_user_email ,
            'paying_user_phone' : response.paying_user_phone ,
            'receiving_user_name' : response.receiving_user_name ,
            'message_to_payer' : response.message_to_payer ,
            'ref_code' : response.ref_code ,
            'ref_collection' : response.ref_collection ,
            'ref_visit_id' : response.ref_visit_id ,
            'ref_route_id' : response.ref_route_id ,
            'status' : response.status ,
            'amount_NOK' : response.amount_NOK ,
            'calc_time' : response.calc_time ,
            'vipps_order_id' : response.vipps_order_id ,
            'info' : {
                'content' : response.info.content ,
                'code' : response.info.code ,
                'ok' : response.info.ok
            }
        }

def UpdateCompany(email_address : str, phone_number : str, billname : str, accountnum : str, companyname : str,
                  companynum : str, companyaddress : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.UpdateCompany(vedbjorn_pb2.Company(
            owner = vedbjorn_pb2.UserContactInfo(
                email_address = email_address ,
                phone_number = phone_number
            ) ,
            billname = billname,
            accountnum = accountnum,
            companyname = companyname,
            companynum = companynum,
            companyaddress = companyaddress
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'owner' : {
                'email_address' : response.owner.email_address ,
                'phone_number' : response.owner.phone_number
            },
            'billname' : response.billname ,
            'accountnum': response.accountnum,
            'companyname': response.companyname,
            'companynum': response.companynum,
            'companyaddress': response.companyaddress,
            'info': {
                'content': response.info.content,
                'code': response.info.code,
                'ok': response.info.ok
            }
        }

def GetCompany(email_address : str, phone_number : str, companyname : str, companynum : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetCompany(vedbjorn_pb2.Company(
            owner = vedbjorn_pb2.UserContactInfo(
                email_address = email_address ,
                phone_number = phone_number
            ) ,
            companyname = companyname,
            companynum = companynum,
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'owner' : {
                'email_address' : response.owner.email_address ,
                'phone_number' : response.owner.phone_number
            },
            'billname' : response.billname ,
            'accountnum': response.accountnum,
            'companyname': response.companyname,
            'companynum': response.companynum,
            'companyaddress': response.companyaddress,
            'info': {
                'content': response.info.content,
                'code': response.info.code,
                'ok': response.info.ok
            }
        }

def GetBatchSellRequest(email_address : str, phone_number : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetBatchSellRequest(vedbjorn_pb2.BatchSellRequest(
            owner = vedbjorn_pb2.UserContactInfo(
                email_address = email_address ,
                phone_number = phone_number
            )
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'owner' : {
                'email_address' : response.owner.email_address ,
                'phone_number' : response.owner.phone_number
            },
            'info': {
                'content': response.info.content,
                'code': response.info.code,
                'ok': response.info.ok
            }
        }

def UpdateBatchSellRequest(email_address : str, phone_number : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.UpdateBatchSellRequest(vedbjorn_pb2.BatchSellRequest(
            owner = vedbjorn_pb2.UserContactInfo(
                email_address = email_address ,
                phone_number = phone_number
            )
        ), metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'owner' : {
                'email_address' : response.owner.email_address ,
                'phone_number' : response.owner.phone_number
            },
            'info': {
                'content': response.info.content,
                'code': response.info.code,
                'ok': response.info.ok
            }
        }

def OrderAdmMassEmails(title : str, text : str, toBuyers : bool, toSellers : bool, toDrivers : bool, emails : list) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.OrderAdmMassEmails(vedbjorn_pb2.AdmMassEmails(
            title = title,
            text = text,
            toBuyers = toBuyers,
            toSellers = toSellers,
            toDrivers = toDrivers,
            emails = emails
        ) ,
        metadata=[('authorization', 'Bearer ' + access_token)])
        return {
            'info' : {
                'content' : response.content,
                'code' : response.code,
                'ok' : response.ok
            }
        }

def GetPrices() :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.GetPrices(
            vedbjorn_pb2.Nothing() ,
            metadata=[('authorization', 'Bearer ' + access_token)])
        ret : list = list()
        for price in response.prices :
            ret.append({
                'county' : price.county ,
                'price' : price.price,
                'product' : price.product
            })
        return ret

def SetPrices(prices : list) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        gprices = vedbjorn_pb2.AllPrices()
        for price in prices :
            gprices.prices.append(vedbjorn_pb2.PriceDefinition(
                county = price['county'] ,
                price = price['price'] ,
                product = price['product']
            ) ,
            metadata=[('authorization', 'Bearer ' + access_token)])
        response = stub.SetPrices(gprices)
        return {
            'info': {
                'content': response.content,
                'code': response.code,
                'ok': response.ok
            }
        }

def GetSeasonOnOrOff() :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        try:
            response = stub.GetSeasonOnOrOff(
                vedbjorn_pb2.Nothing(),
                metadata=[('authorization', 'Bearer ' + access_token)]
            )
        except Exception as e :
            print('Failed at : response = stub.GetSeasonOnOrOff(vedbjorn_pb2.Nothing())')
            print('\tException was : ')
            print(e)
            return 'on' # default
        return response.value

def SetSeasonOnOrOff(on_or_off : str) :
    with grpc.secure_channel(host, cred) as channel:
        stub = vedbjorn_pb2_grpc.VedbjornFunctionsStub(channel)
        response = stub.SetSeasonOnOrOff(
            vedbjorn_pb2.Name(value = on_or_off),
            metadata=[('authorization', 'Bearer ' + access_token)])

        return {
            'info': {
                'content': response.content,
                'code': response.code,
                'ok': response.ok
            }
        }

