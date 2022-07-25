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

func (util *DbUtil) PrepareQuery(context utilities.AppContext,Db *sql.DB,utilitytype string) error{
	now := time.Now().UTC()
	presentday := now.AddDate(0,0,-1)
	previousdays := presentday.AddDate(0, 0, -14)
	//todate := presentday.Format("2006-01-02")+" 23:59:59"
	fromdate := previousdays.Format("2006-01-02")+" 23:59:59"
	//fmt.Printf("todate ::%v\n",todate)
	if utilitytype == "all"{
		err := util.RangeAll(context,Db,fromdate)
		if err != nil{
			fmt.Printf("error when inserting records to history tables%v",err)
			context.Logger.Info("error when inserting records to history tables%v",err)
			return err
		}
	/* else if utilitytype == "lastmonth"{
		err := util.WithinRange(context,Db,fromdate,todate)
		if err != nil{
			fmt.Printf("error when inserting range of records to history tables%v\n",err)
			context.Logger.Info("error when inserting range of records to history tables%v\n",err)
			return err
		}
	}*/ } else if utilitytype == "deleteall"{
		err := util.DeleteAll(context,Db,fromdate)
		if err != nil{
			fmt.Printf("error when inserting range of records to history tables%v\n",err)
			context.Logger.Info("error when inserting range of records to history tables%v\n",err)
			return err
		}
	}
	/*  else if utilitytype == "deletebeforeweek"{
		err := util.DeleteWithinRange(context,Db,fromdate,todate)
		if err != nil{
			fmt.Printf("error when inserting range of records to history tables%v\n",err)
			context.Logger.Info("error when inserting range of records to history tables%v\n",err)
			return err
		}
	}*/

	return nil
}

func (util *DbUtil) RangeAll(context utilities.AppContext,Db *sql.DB,last14daydate string) error{
	context.Logger.Info("Date range is: %v\n",last14daydate)
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

func (util *DbUtil) WithinRange(context utilities.AppContext,Db *sql.DB,last14daydate,presentDate string) error{
	err := util.InsertLastMonthHistory(context,Db,last14daydate,presentDate,"amf_message_history")
	if err != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else{
			fmt.Printf("error when inserting message history record%v\n",err)
			context.Logger.Info("error when inserting message history record%v\n",err)
			return err
		}
	}
	time.Sleep(5 * time.Second)
	serr := util.InsertLastMonthHistory(context,Db,last14daydate,presentDate,"amf_session_history")
	if serr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else {
			fmt.Printf("error when inserting session history record%v\n", err)
			context.Logger.Info("error when inserting session history record%v\n", err)
			return serr
		}
	}
	time.Sleep(5 * time.Second)
	srerr := util.InsertLastMonthHistory(context,Db,last14daydate,presentDate,"amf_session_rel_history")
	if srerr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else {
			fmt.Printf("error when inserting session relation record%v\n", srerr)
			context.Logger.Info("error when inserting session relation record%v\n", srerr)
			return srerr
		}
	}
	time.Sleep(5 * time.Second)
	eventerr := util.InsertLastMonthHistory(context,Db,last14daydate,presentDate,"amf_event_history")
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
	dmerr := util.DeleteHistory(context,Db,"",last14daydate,"amf_message")
	if dmerr != nil{
		fmt.Printf("error when deleting message table%v",dmerr)
		context.Logger.Info("error when deleting message table%v",dmerr)
		return dmerr
	}

	dserr := util.DeleteHistory(context,Db,"",last14daydate,"amf_session")
	if dserr != nil{
		fmt.Printf("error when deleting session table%v",dserr)
		context.Logger.Info("error when deleting session table%v",dserr)
		return dserr
	}

	dsrerr := util.DeleteHistory(context,Db,"",last14daydate,"amf_session_rel")
	if dsrerr != nil{
		fmt.Printf("error when deleting session rel table%v",dsrerr)
		context.Logger.Info("error when deleting session rel table%v",dsrerr)
		return dsrerr
	}

	deerr := util.DeleteHistory(context,Db,"",last14daydate,"amf_event")
	if deerr != nil{
		fmt.Printf("error when deleting event table%v",deerr)
		context.Logger.Info("error when deleting event table%v",deerr)
		return deerr
	}
	return nil
}

func (util *DbUtil) DeleteWithinRange(context utilities.AppContext,Db *sql.DB,last14daydate,presentDate string) error{
	dmerr := util.DeleteHistory(context,Db,last14daydate,presentDate,"amf_message")
	if dmerr != nil{
		fmt.Printf("error when deleting message table%v",dmerr)
		context.Logger.Info("error when deleting message table%v",dmerr)
		return dmerr
	}

	dserr := util.DeleteHistory(context,Db,last14daydate,presentDate,"amf_session")
	if dserr != nil{
		fmt.Printf("error when deleting session table%v",dserr)
		context.Logger.Info("error when deleting session table%v",dserr)
		return dserr
	}

	dsrerr := util.DeleteHistory(context,Db,last14daydate,presentDate,"amf_session_rel")
	if dsrerr != nil{
		fmt.Printf("error when deleting session rel table%v",dsrerr)
		context.Logger.Info("error when deleting session rel table%v",dsrerr)
		return dsrerr
	}

	deerr := util.DeleteHistory(context,Db,last14daydate,presentDate,"amf_event")
	if deerr != nil{
		fmt.Printf("error when deleting event table%v",deerr)
		context.Logger.Info("error when deleting event table%v",deerr)
		return deerr
	}
	return nil
}