package dbutility

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
	"github.com/google/uuid"
)

type DbUtil struct{
	Db    *sql.DB
	Timezone string
}

type MessageDetails struct {
	MessageId uuid.UUID `json:"message_id"`
	Sender  string `json:"sender"`
	Receiver  string `json:"receiver"`
	MsgType  string `json:"msg_type"`
	ControlNo  string `json:"control_no"`
	FileName  string `json:"file_name"`
	FilePath string `json:"file_path"`
	FileSize string `json:"file_size"`
	Origin string `json:"origin"`
	CreatedBy string `json:"created_by"`
	WorkflowId uuid.UUID `json:"workflow_id"`
	SessionId uuid.UUID `json:"session_id"`
	ParentId uuid.UUID `json:"parent_id"`
	ReferenceId uuid.UUID `json:"reference_id"`
	Status string `json:"status"`
	CreateTime  string `json:"create_time"`
	StatusTime string `json:"status_time"`
	StatusTime1 string `json:"status_time1"`
	CanRequeue string `json:"can_requeue"`
	CanReprocess string `json:"can_reprocess"`
	CanReqAndRep string `json:"can_req_and_rep"`
	SiteId string `json:"site_id"`
	NodeId string `json:"node_id"`
	FileType string `json:"file_type"`
	DataType string `json:"data_type"`
	Contents string `json:"contents"`
}


type SessionDetails struct {
	SessionId uuid.UUID `json:"session_id"`
	SessionStart string `json:"session_start"`
	SessionEnd string `json:"session_end"`
	WorkflowName string `json:"workflow_name"`
	InstanceId string `json:"instance_id"`
	Username string `json:"user"`
	Status string `json:"status"`
	CreateTime string `json:"create_time"`
	CreatedBy string `json:"created_by"`
	SiteId string `json:"site_id"`
	NodeId string `json:"node_id"`
}

type SessionRelDetails struct {
	RelationId uuid.UUID `json:"relation_id"`
	SessionId uuid.UUID `json:"session_id"`
	MessageId uuid.UUID `json:"message_id"`
	RelType string `json:"rel_type"`
	CreateTime string `json:"create_time"`
	CreatedBy string `json:"created_by"`
}

type EventDetails struct {
	EventId uuid.UUID `json:"event_id"`
	SessionId uuid.UUID `json:"session_id"`
	MessageId interface{} `json:"message_id"`
	ActionId  uuid.UUID `json:"action_id"`
	CreatedBy string `json:"created_by"`
	Level interface{} `json:"level"`
	Text string `json:"text"`
	Status string `json:"status"`
	CreateTime string `json:"create_time"`
}

func (util *DbUtil) PrepareQuery(Db *sql.DB,utilitytype string) error{
	now := time.Now().UTC()
	lastmonth := now.AddDate(0, -1, 0)
	currentYear, lastMonth, _ := lastmonth.Date()
	currentLocation := now.Location()
	firstOfMonth := time.Date(currentYear, lastMonth, 1, 0, 0, 0, 0, currentLocation)
	lastOfMonth := time.Date(currentYear, lastMonth+1, 0, 23, 59, 59, 999999999, currentLocation)
	if utilitytype == "all"{
		err := util.RangeAll(Db,lastOfMonth)
		if err != nil{
			fmt.Printf("error when inserting records to history tables%v",err)
			return err
		}
	} else if utilitytype == "lastmonth"{
		err := util.WithinRange(Db,firstOfMonth,lastOfMonth)
		if err != nil{
			fmt.Printf("error when inserting range of records to history tables%v\n",err)
			return err
		}
	}

	return nil
}

func (util *DbUtil) RangeAll(Db *sql.DB,lastOfMonth time.Time) error{
	fmt.Printf("Date range is: %v\n",lastOfMonth)
	err:= util.InsertToHistoryTable(Db,lastOfMonth,"amf_message_history")
	if err != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else{
			fmt.Printf("error when inserting records to message history table%v",err)
			return err
		}

	}  else {
		derr := util.DeleteHistory(Db,"",lastOfMonth.Format("2006-01-02 15:04:05"),"amf_message")
		if derr != nil{
			fmt.Printf("error when deleting message table%v",err)
			return err
		}
	}
	time.Sleep(5 * time.Second)
	serr:= util.InsertToHistoryTable(Db,lastOfMonth,"amf_session_history")
	if serr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else{
			fmt.Printf("error when inserting records to session history table%v",serr)
			return serr
		}

	} else {
		derr := util.DeleteHistory(Db,"",lastOfMonth.Format("2006-01-02 15:04:05"),"amf_session")
		if derr != nil{
			fmt.Printf("error when deleting session table%v",err)
			return err
		}
	}
	time.Sleep(5 * time.Second)
	srerr:= util.InsertToHistoryTable(Db,lastOfMonth,"amf_session_rel_history")
	if srerr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else{
			fmt.Printf("error when inserting records to session rel history table%v",srerr)
			return srerr
		}
	}else {
		derr := util.DeleteHistory(Db,"",lastOfMonth.Format("2006-01-02 15:04:05"),"amf_session_rel")
		if derr != nil{
			fmt.Printf("error when deleting session rel table%v",err)
			return err
		}
	}
	time.Sleep(5 * time.Second)
	eerr:= util.InsertToHistoryTable(Db,lastOfMonth,"amf_event_history")
	if eerr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else{
			fmt.Printf("error when inserting records to event history table%v",eerr)
			return eerr
		}
	} else {
		derr := util.DeleteHistory(Db,"",lastOfMonth.Format("2006-01-02 15:04:05"),"amf_event")
		if derr != nil{
			fmt.Printf("error when deleting event table%v",err)
			return err
		}
	}
	return nil
}

func (util *DbUtil) WithinRange(Db *sql.DB,firstOfMonth,lastOfMonth time.Time) error{
	firstOfMonth1 := firstOfMonth.Format("2006-01-02 15:04:05")
	lastOfMonth1 := lastOfMonth.Format("2006-01-02 15:04:05")
	err := util.InsertLastMonthHistory(Db,firstOfMonth1,lastOfMonth1,"amf_message_history")
	if err != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else{
			fmt.Printf("error when inserting message history record%v\n",err)
			return err
		}
	}  else {
		derr := util.DeleteHistory(Db,firstOfMonth1,lastOfMonth1,"amf_message")
		if derr != nil{
			fmt.Printf("error when deleting message table%v",err)
			return err
		}
	}
	time.Sleep(5 * time.Second)
	serr := util.InsertLastMonthHistory(Db,firstOfMonth1,lastOfMonth1,"amf_session_history")
	if serr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else {
			fmt.Printf("error when inserting session history record%v\n", err)
			return serr
		}
	} else {
		derr := util.DeleteHistory(Db,firstOfMonth1,lastOfMonth1,"amf_session")
		if derr != nil{
			fmt.Printf("error when deleting session table%v",err)
			return err
		}
	}
	time.Sleep(5 * time.Second)
	srerr := util.InsertLastMonthHistory(Db,firstOfMonth1,lastOfMonth1,"amf_session_rel")
	if srerr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else {
			fmt.Printf("error when inserting session relation record%v\n", srerr)
			return srerr
		}
	} else {
		derr := util.DeleteHistory(Db,firstOfMonth1,lastOfMonth1,"amf_session_rel")
		if derr != nil{
			fmt.Printf("error when deleting session rel table%v",err)
			return err
		}
	}
	time.Sleep(5 * time.Second)
	eventerr := util.InsertLastMonthHistory(Db,firstOfMonth1,lastOfMonth1,"amf_event_history")
	if eventerr != nil{
		if strings.Contains(err.Error(),"duplicate key value"){
		} else {
			fmt.Printf("error when inserting event history record%v\n", eventerr)
			return eventerr
		}
	} else {
		derr := util.DeleteHistory(Db,firstOfMonth1,lastOfMonth1,"amf_event")
		if derr != nil{
			fmt.Printf("error when deleting event table%v",err)
			return err
		}
	}
	return nil
}
