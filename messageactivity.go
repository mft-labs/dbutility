package dbutility

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
	"amfui/utilities"
)

type DbUtil struct{
	Db    *sql.DB
	Timezone string
}

func (util *DbUtil) PrepareQuery(context utilities.AppContext,Db *sql.DB,utilitytype,startdate,enddate string,clean,validatemain,validatehistory bool) error{
	startdate = startdate +" 00:00:00"
	enddate = enddate+" 23:59:59"
	now := time.Now().UTC()
	presentday := now.AddDate(0,0,-1)
	previousdays := presentday.AddDate(0, 0, -14)
	fromdate := previousdays.Format("2006-01-02")+" 23:59:59"
	if clean == false && utilitytype == "all" && validatemain == false && validatehistory == false{
		fmt.Printf("Date range is::%v\n", fromdate)
		context.Logger.Info("Date range is::%v\n", fromdate)
		err := util.RangeAll(context, Db, fromdate)
		if err != nil {
			fmt.Printf("error when inserting records to history tables%v", err)
			context.Logger.Info("error when inserting records to history tables%v", err)
			return err
		}
	} else if utilitytype == "" && clean == false && validatemain == false && validatehistory == false{
		fmt.Printf("Start Date is::%v\n", startdate)
		fmt.Printf("End Date is::%v\n", enddate)
		context.Logger.Info("Start Date is::%v\n", startdate)
		context.Logger.Info("End Date is::%v\n", enddate)
		err := util.WithinRange(context,Db,startdate,enddate)
		if err != nil{
			fmt.Printf("error when inserting range of records to history tables%v\n",err)
			context.Logger.Info("error when inserting range of records to history tables%v\n",err)
			return err
		}
	 } else if clean && utilitytype == "all" && validatemain == false && validatehistory == false{
		fmt.Printf("Date range is::%v\n", fromdate)
		context.Logger.Info("Date range is::%v\n", fromdate)
		err := util.DeleteAll(context,Db,fromdate)
		if err != nil{
			fmt.Printf("error when inserting range of records to history tables%v\n",err)
			context.Logger.Info("error when inserting range of records to history tables%v\n",err)
			return err
		}
	} else if clean && utilitytype == "" && validatemain == false && validatehistory == false{
		fmt.Printf("Start Date is::%v\n", startdate)
		fmt.Printf("End Date is::%v\n", enddate)
		context.Logger.Info("Start Date is::%v\n", startdate)
		context.Logger.Info("End Date is::%v\n", enddate)
		err := util.DeleteWithinRange(context,Db,startdate,enddate)
		if err != nil{
			fmt.Printf("error when inserting range of records to history tables%v\n",err)
			context.Logger.Info("error when inserting range of records to history tables%v\n",err)
			return err
		}
	} else if validatemain && utilitytype == "all"{
		err := util.ValidateAll(context,Db,fromdate,"main")
		if err != nil{

		}
	} else if validatemain && utilitytype == ""{
		err := util.ValidateWithinRange(context,Db,startdate,enddate,"main")
		if err != nil{

		}
	}  else if validatehistory && utilitytype == "all"{
		err := util.ValidateAll(context,Db,fromdate,"history")
		if err != nil{

		}
	} else if validatehistory && utilitytype == ""{
		err := util.ValidateWithinRange(context,Db,startdate,enddate,"history")
		if err != nil{

		}
	}

	return nil
}

func (util *DbUtil) RangeAll(context utilities.AppContext,Db *sql.DB,last14daydate string) error{
	context.Logger.Info("Date range is: %v\n",last14daydate)
	count,cmerr := util.CheckCount(context,Db,"amf_message","",last14daydate)
	if cmerr != nil{
	}
	fmt.Printf("Count from message table is: %v\n",count)
	context.Logger.Info("Count from message table is: %v\n",count)
	err:= util.InsertToHistoryTable(context,Db,last14daydate,"amf_message_history")
	if err != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else{
			fmt.Printf("error when inserting records to message history table%v",err)
			context.Logger.Info("error when inserting records to message history table%v",err)
			return err
		}

	}
	time.Sleep(5 * time.Second)
	scount,cserr := util.CheckCount(context,Db,"amf_session","",last14daydate)
	if cserr != nil{
	}
	fmt.Printf("Count from session table is: %v\n",scount)
	context.Logger.Info("Count from session table is: %v\n",scount)
	serr:= util.InsertToHistoryTable(context,Db,last14daydate,"amf_session_history")
	if serr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else{
			fmt.Printf("error when inserting records to session history table%v",serr)
			context.Logger.Info("error when inserting records to session history table%v",serr)
			return serr
		}

	}
	time.Sleep(5 * time.Second)
	srcount,csrerr := util.CheckCount(context,Db,"amf_session_rel","",last14daydate)
	if csrerr != nil{
	}
	fmt.Printf("Count from session relation table is: %v\n",srcount)
	context.Logger.Info("Count from session relation table is: %v\n",srcount)
	srerr:= util.InsertToHistoryTable(context,Db,last14daydate,"amf_session_rel_history")
	if srerr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else{
			fmt.Printf("error when inserting records to session rel history table%v",srerr)
			context.Logger.Info("error when inserting records to session rel history table%v",srerr)
			return srerr
		}
	}
	time.Sleep(5 * time.Second)
	ercount,ceerr := util.CheckCount(context,Db,"amf_event","",last14daydate)
	if ceerr != nil{
	}
	fmt.Printf("Count from event table is: %v\n",ercount)
	context.Logger.Info("Count from event table is: %v\n",ercount)
	eerr:= util.InsertToHistoryTable(context,Db,last14daydate,"amf_event_history")
	if eerr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else{
			fmt.Printf("error when inserting records to event history table%v",eerr)
			context.Logger.Info("error when inserting records to event history table%v",eerr)
			return eerr
		}
	}
	return nil
}

func (util *DbUtil) WithinRange(context utilities.AppContext,Db *sql.DB,startdate,enddate string) error{
	count,cmerr := util.CheckCount(context,Db,"amf_message",startdate,enddate)
	if cmerr != nil{
	}
	fmt.Printf("Count from message table is: %v\n",count)
	context.Logger.Info("Count from message table is: %v\n",count)
	err := util.InsertLastMonthHistory(context,Db,startdate,enddate,"amf_message_history")
	if err != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else{
			fmt.Printf("error when inserting message history record%v\n",err)
			context.Logger.Info("error when inserting message history record%v\n",err)
			return err
		}
	}
	time.Sleep(5 * time.Second)
	scount,cserr := util.CheckCount(context,Db,"amf_session",startdate,enddate)
	if cserr != nil{
	}
	fmt.Printf("Count from session table is: %v\n",scount)
	context.Logger.Info("Count from session table is: %v\n",scount)
	serr := util.InsertLastMonthHistory(context,Db,startdate,enddate,"amf_session_history")
	if serr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else {
			fmt.Printf("error when inserting session history record%v\n", err)
			context.Logger.Info("error when inserting session history record%v\n", err)
			return serr
		}
	}
	time.Sleep(5 * time.Second)
	srcount,csrerr := util.CheckCount(context,Db,"amf_session_rel",startdate,enddate)
	if csrerr != nil{
	}
	fmt.Printf("Count from session relation table is: %v\n",srcount)
	context.Logger.Info("Count from session relation table is: %v\n",srcount)

	srerr := util.InsertLastMonthHistory(context,Db,startdate,enddate,"amf_session_rel_history")
	if srerr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else {
			fmt.Printf("error when inserting session relation record%v\n", srerr)
			context.Logger.Info("error when inserting session relation record%v\n", srerr)
			return srerr
		}
	}
	time.Sleep(5 * time.Second)
	ercount,ceerr := util.CheckCount(context,Db,"amf_event",startdate,enddate)
	if ceerr != nil{
	}
	fmt.Printf("Count from event table is: %v\n",ercount)
	context.Logger.Info("Count from event table is: %v\n",ercount)
	eventerr := util.InsertLastMonthHistory(context,Db,startdate,enddate,"amf_event_history")
	if eventerr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else {
			fmt.Printf("error when inserting event history record%v\n", eventerr)
			context.Logger.Info("error when inserting event history record%v\n", eventerr)
			return eventerr
		}
	}
	return nil
}

func (util *DbUtil) DeleteAll(context utilities.AppContext,Db *sql.DB,last14daydate string) error{
	count,cmerr := util.CheckCount(context,Db,"amf_message","",last14daydate)
	if cmerr != nil{
	}
	fmt.Printf("Count from message table is: %v\n",count)
	context.Logger.Info("Count from message table is: %v\n",count)

	dmerr := util.DeleteHistory(context,Db,"",last14daydate,"amf_message")
	if dmerr != nil{
		fmt.Printf("error when deleting message table%v",dmerr)
		context.Logger.Info("error when deleting message table%v",dmerr)
		return dmerr
	}

	scount,cserr := util.CheckCount(context,Db,"amf_session","",last14daydate)
	if cserr != nil{
	}
	fmt.Printf("Count from session table is: %v\n",scount)
	context.Logger.Info("Count from session table is: %v\n",scount)

	dserr := util.DeleteHistory(context,Db,"",last14daydate,"amf_session")
	if dserr != nil{
		fmt.Printf("error when deleting session table%v",dserr)
		context.Logger.Info("error when deleting session table%v",dserr)
		return dserr
	}

	srcount,csrerr := util.CheckCount(context,Db,"amf_session_rel","",last14daydate)
	if csrerr != nil{
	}
	fmt.Printf("Count from session relation table is: %v\n",srcount)
	context.Logger.Info("Count from session relation table is: %v\n",srcount)
	dsrerr := util.DeleteHistory(context,Db,"",last14daydate,"amf_session_rel")
	if dsrerr != nil{
		fmt.Printf("error when deleting session rel table%v",dsrerr)
		context.Logger.Info("error when deleting session rel table%v",dsrerr)
		return dsrerr
	}

	ercount,ceerr := util.CheckCount(context,Db,"amf_event","",last14daydate)
	if ceerr != nil{
	}
	fmt.Printf("Count from event table is: %v\n",ercount)
	context.Logger.Info("Count from event table is: %v\n",ercount)
	deerr := util.DeleteHistory(context,Db,"",last14daydate,"amf_event")
	if deerr != nil{
		fmt.Printf("error when deleting event table%v",deerr)
		context.Logger.Info("error when deleting event table%v",deerr)
		return deerr
	}
	return nil
}

func (util *DbUtil) DeleteWithinRange(context utilities.AppContext,Db *sql.DB,last14daydate,presentDate string) error{
	count,cmerr := util.CheckCount(context,Db,"amf_message",last14daydate,presentDate)
	if cmerr != nil{
	}
	fmt.Printf("Count from message table is: %v\n",count)
	context.Logger.Info("Count from message table is: %v\n",count)
	dmerr := util.DeleteHistory(context,Db,last14daydate,presentDate,"amf_message")
	if dmerr != nil{
		fmt.Printf("error when deleting message table%v",dmerr)
		context.Logger.Info("error when deleting message table%v",dmerr)
		return dmerr
	}
	scount,cserr := util.CheckCount(context,Db,"amf_session",last14daydate,presentDate)
	if cserr != nil{
	}
	fmt.Printf("Count from session table is: %v\n",scount)
	context.Logger.Info("Count from session table is: %v\n",scount)
	dserr := util.DeleteHistory(context,Db,last14daydate,presentDate,"amf_session")
	if dserr != nil{
		fmt.Printf("error when deleting session table%v",dserr)
		context.Logger.Info("error when deleting session table%v",dserr)
		return dserr
	}

	srcount,csrerr := util.CheckCount(context,Db,"amf_session_rel",last14daydate,presentDate)
	if csrerr != nil{
	}
	fmt.Printf("Count from session relation table is: %v\n",srcount)
	context.Logger.Info("Count from session relation table is: %v\n",srcount)
	dsrerr := util.DeleteHistory(context,Db,last14daydate,presentDate,"amf_session_rel")
	if dsrerr != nil{
		fmt.Printf("error when deleting session rel table%v",dsrerr)
		context.Logger.Info("error when deleting session rel table%v",dsrerr)
		return dsrerr
	}

	ercount,ceerr := util.CheckCount(context,Db,"amf_event",last14daydate,presentDate)
	if ceerr != nil{
	}
	fmt.Printf("Count from event table is: %v\n",ercount)
	context.Logger.Info("Count from event table is: %v\n",ercount)
	deerr := util.DeleteHistory(context,Db,last14daydate,presentDate,"amf_event")
	if deerr != nil{
		fmt.Printf("error when deleting event table%v",deerr)
		context.Logger.Info("error when deleting event table%v",deerr)
		return deerr
	}
	return nil
}


func (util *DbUtil) ValidateAll(context utilities.AppContext,Db *sql.DB,last14daydate,tabletype string) error{
	var tablename string
	var sessiontable string
	var sessionreltable string
	var eventtable string
	if tabletype == "main"{
		tablename = "amf_message"
		sessiontable = "amf_session"
		sessionreltable = "amf_session_rel"
		eventtable = "amf_event"
	} else {
		tablename = "amf_message_history"
		sessiontable = "amf_session_history"
		sessionreltable = "amf_session_rel_history"
		eventtable = "amf_event_history"
	}
	count,cmerr := util.CheckCount(context,Db,tablename,"",last14daydate)
	if cmerr != nil{
	}
	fmt.Printf("Count from %v table is: %v\n",tablename,count)
	context.Logger.Info("Count from %v table is: %v\n",tablename,count)
	count1,cmerr := util.CheckCount(context,Db,sessiontable,"",last14daydate)
	if cmerr != nil{
	}
	fmt.Printf("Count from %v table is: %v\n",sessiontable,count1)
	context.Logger.Info("Count from %v table is: %v\n",sessiontable,count1)
	count2,cmerr := util.CheckCount(context,Db,sessionreltable,"",last14daydate)
	if cmerr != nil{
	}
	fmt.Printf("Count from %v table is: %v\n",sessionreltable,count2)
	context.Logger.Info("Count from %v table is: %v\n",sessionreltable,count2)
	count3,cmerr := util.CheckCount(context,Db,eventtable,"",last14daydate)
	if cmerr != nil{
	}
	fmt.Printf("Count from %v table is: %v\n",eventtable,count3)
	context.Logger.Info("Count from %v table is: %v\n",eventtable,count3)
	util.CheckDistinctSenderWithCount(context,Db,tablename,"",last14daydate)
	util.CheckDistinctReceiverWithCount(context,Db,tablename,"",last14daydate)
	return nil
}

func (util *DbUtil) ValidateWithinRange(context utilities.AppContext,Db *sql.DB,startdate,enddate,tabletype string) error{
	var tablename string
	var sessiontable string
	var sessionreltable string
	var eventtable string
	if tabletype == "main"{
		tablename = "amf_message"
		sessiontable = "amf_session"
		sessionreltable = "amf_session_rel"
		eventtable = "amf_event"
	} else {
		tablename = "amf_message_history"
		sessiontable = "amf_session_history"
		sessionreltable = "amf_session_rel_history"
		eventtable = "amf_event_history"
	}
	count,cmerr := util.CheckCount(context,Db,tablename,startdate,enddate)
	if cmerr != nil{
	}
	fmt.Printf("Count from message table is: %v\n",count)
	context.Logger.Info("Count from message table is: %v\n",count)
	count1,cmerr := util.CheckCount(context,Db,sessiontable,startdate,enddate)
	if cmerr != nil{
	}
	fmt.Printf("Count from %v table is: %v\n",sessiontable,count1)
	context.Logger.Info("Count from %v table is: %v\n",sessiontable,count1)
	count2,cmerr := util.CheckCount(context,Db,sessionreltable,startdate,enddate)
	if cmerr != nil{
	}
	fmt.Printf("Count from %v table is: %v\n",sessionreltable,count2)
	context.Logger.Info("Count from %v table is: %v\n",sessionreltable,count2)
	count3,cmerr := util.CheckCount(context,Db,eventtable,startdate,enddate)
	if cmerr != nil{
	}
	fmt.Printf("Count from %v table is: %v\n",eventtable,count3)
	context.Logger.Info("Count from %v table is: %v\n",eventtable,count3)
	util.CheckDistinctSenderWithCount(context,Db,tablename,startdate,enddate)
	util.CheckDistinctReceiverWithCount(context,Db,tablename,startdate,enddate)
	return nil
}