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
	lastmonth := now.AddDate(0, -1, 0)
	currentYear, lastMonth, _ := lastmonth.Date()
	currentLocation := now.Location()
	firstOfMonth := time.Date(currentYear, lastMonth, 1, 0, 0, 0, 0, currentLocation)
	lastOfMonth := time.Date(currentYear, lastMonth+1, 0, 23, 59, 59, 999999999, currentLocation)
	if utilitytype == "all"{
		err := util.RangeAll(context,Db,lastOfMonth)
		if err != nil{
			fmt.Printf("error when inserting records to history tables%v",err)
			context.Logger.Info("error when inserting records to history tables%v",err)
			return err
		}
	} else if utilitytype == "lastmonth"{
		err := util.WithinRange(context,Db,firstOfMonth,lastOfMonth)
		if err != nil{
			fmt.Printf("error when inserting range of records to history tables%v\n",err)
			context.Logger.Info("error when inserting range of records to history tables%v\n",err)
			return err
		}
	}

	return nil
}

func (util *DbUtil) RangeAll(context utilities.AppContext,Db *sql.DB,lastOfMonth time.Time) error{
	context.Logger.Info("Date range is: %v\n",lastOfMonth)
	err:= util.InsertToHistoryTable(context,Db,lastOfMonth,"amf_message_history")
	if err != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else{
			fmt.Printf("error when inserting records to message history table%v",err)
			context.Logger.Info("error when inserting records to message history table%v",err)
			return err
		}

	}  else {
		derr := util.DeleteHistory(context,Db,"",lastOfMonth.Format("2006-01-02 15:04:05"),"amf_message")
		if derr != nil{
			fmt.Printf("error when deleting message table%v",err)
			context.Logger.Info("error when deleting message table%v",err)
			return err
		}
	}
	time.Sleep(5 * time.Second)
	serr:= util.InsertToHistoryTable(context,Db,lastOfMonth,"amf_session_history")
	if serr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else{
			fmt.Printf("error when inserting records to session history table%v",serr)
			context.Logger.Info("error when inserting records to session history table%v",serr)
			return serr
		}

	} else {
		derr := util.DeleteHistory(context,Db,"",lastOfMonth.Format("2006-01-02 15:04:05"),"amf_session")
		if derr != nil{
			fmt.Printf("error when deleting session table%v",err)
			context.Logger.Info("error when deleting session table%v",err)
			return err
		}
	}
	time.Sleep(5 * time.Second)
	srerr:= util.InsertToHistoryTable(context,Db,lastOfMonth,"amf_session_rel_history")
	if srerr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else{
			fmt.Printf("error when inserting records to session rel history table%v",srerr)
			context.Logger.Info("error when inserting records to session rel history table%v",srerr)
			return srerr
		}
	}else {
		derr := util.DeleteHistory(context,Db,"",lastOfMonth.Format("2006-01-02 15:04:05"),"amf_session_rel")
		if derr != nil{
			fmt.Printf("error when deleting session rel table%v",err)
			context.Logger.Info("error when deleting session rel table%v",err)
			return err
		}
	}
	time.Sleep(5 * time.Second)
	eerr:= util.InsertToHistoryTable(context,Db,lastOfMonth,"amf_event_history")
	if eerr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else{
			fmt.Printf("error when inserting records to event history table%v",eerr)
			context.Logger.Info("error when inserting records to event history table%v",eerr)
			return eerr
		}
	} else {
		derr := util.DeleteHistory(context,Db,"",lastOfMonth.Format("2006-01-02 15:04:05"),"amf_event")
		if derr != nil{
			fmt.Printf("error when deleting event table%v",err)
			context.Logger.Info("error when deleting event table%v",err)
			return err
		}
	}
	return nil
}

func (util *DbUtil) WithinRange(context utilities.AppContext,Db *sql.DB,firstOfMonth,lastOfMonth time.Time) error{
	firstOfMonth1 := firstOfMonth.Format("2006-01-02 15:04:05")
	lastOfMonth1 := lastOfMonth.Format("2006-01-02 15:04:05")
	err := util.InsertLastMonthHistory(context,Db,firstOfMonth1,lastOfMonth1,"amf_message_history")
	if err != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else{
			fmt.Printf("error when inserting message history record%v\n",err)
			context.Logger.Info("error when inserting message history record%v\n",err)
			return err
		}
	}  else {
		derr := util.DeleteHistory(context,Db,firstOfMonth1,lastOfMonth1,"amf_message")
		if derr != nil{
			fmt.Printf("error when deleting message table%v",err)
			context.Logger.Info("error when deleting message table%v",err)
			return err
		}
	}
	time.Sleep(5 * time.Second)
	serr := util.InsertLastMonthHistory(context,Db,firstOfMonth1,lastOfMonth1,"amf_session_history")
	if serr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else {
			fmt.Printf("error when inserting session history record%v\n", err)
			context.Logger.Info("error when inserting session history record%v\n", err)
			return serr
		}
	} else {
		derr := util.DeleteHistory(context,Db,firstOfMonth1,lastOfMonth1,"amf_session")
		if derr != nil{
			fmt.Printf("error when deleting session table%v",err)
			context.Logger.Info("error when deleting session table%v",err)
			return err
		}
	}
	time.Sleep(5 * time.Second)
	srerr := util.InsertLastMonthHistory(context,Db,firstOfMonth1,lastOfMonth1,"amf_session_rel_history")
	if srerr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else {
			fmt.Printf("error when inserting session relation record%v\n", srerr)
			context.Logger.Info("error when inserting session relation record%v\n", srerr)
			return srerr
		}
	} else {
		derr := util.DeleteHistory(context,Db,firstOfMonth1,lastOfMonth1,"amf_session_rel")
		if derr != nil{
			fmt.Printf("error when deleting session rel table%v",err)
			context.Logger.Info("error when deleting session rel table%v",err)
			return err
		}
	}
	time.Sleep(5 * time.Second)
	eventerr := util.InsertLastMonthHistory(context,Db,firstOfMonth1,lastOfMonth1,"amf_event_history")
	if eventerr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else {
			fmt.Printf("error when inserting event history record%v\n", eventerr)
			context.Logger.Info("error when inserting event history record%v\n", eventerr)
			return eventerr
		}
	} else {
		derr := util.DeleteHistory(context,Db,firstOfMonth1,lastOfMonth1,"amf_event")
		if derr != nil{
			fmt.Printf("error when deleting event table%v",err)
			context.Logger.Info("error when deleting event table%v",err)
			return err
		}
	}
	return nil
}
