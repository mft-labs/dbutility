package dbutility

import (
	"database/sql"
	"fmt"
	"time"
)

const (
	SELECT_MESSAGES = "select sender,receiver,msg_type,control_no,file_name,file_type,file_path,workflow_id,session_id,parent_id,doc_count,origin,reference_id,status,status_time::timestamptz,can_requeue,can_reprocess,create_time::timestamptz,created_by,message_id,file_size,site_id,node_id,can_req_and_rep,data_type,contents from amf_message msg "
	INSERT_INTO_MESSAGE_HISTORY = "insert into amf_message_history (sender,receiver,msg_type,control_no,file_name,file_type,file_path,workflow_id,session_id,parent_id,doc_count,origin,reference_id,status,status_time,can_requeue,can_reprocess,create_time,created_by,message_id,file_size,site_id,node_id,can_req_and_rep,data_type,contents) "

	SELECT_SESSIONS = "select session_id,session_start,session_end,workflow_name,instance_id,username,status,create_time,created_by,site_id,node_id from amf_session"
	INSERT_INTO_SESSION_HISTORY = "insert into amf_session_history (session_id,session_start,session_end,workflow_name,instance_id,username,status,create_time,created_by,site_id,node_id)"

	SELECT_SESSION_REL = "select relation_id,session_id,message_id,rel_type,create_time,created_by from amf_session_rel"
	INSERT_INTO_SESSION_REL_HISTORY = "insert into amf_session_rel_history (relation_id,session_id,message_id,rel_type,create_time,created_by)"

	SELECT_EVENT = "select event_id,level,message_id,session_id,action_id,text,status,create_time,created_by from amf_event"
	INSERT_INTO_EVENT_HISTORY = "insert into amf_event_history (event_id,level,message_id,session_id,action_id,text,status,create_time,created_by)"
)

func (util *DbUtil) DeleteHistory(Db *sql.DB,fromdate,todate,tablename string) error{
	if fromdate == ""{
		query := "delete from "+tablename+" where create_time <= $1"
		resp, err := Db.Exec(query,todate)
		fmt.Printf("Delete response for %v is: %v\n",tablename,resp)
		if err != nil{
			return err
		}
	} else {
		query := "delete from "+tablename+" where create_time >= $1 and create_time <= $2"
		resp, err := Db.Exec(query,fromdate,todate)
		fmt.Printf("Delete response for %v is: %v\n",tablename,resp)
		if err != nil{
			return err
		}
	}

	return nil
}

func (util *DbUtil) InsertToHistoryTable(Db *sql.DB,then time.Time,tablename string) error{
	if tablename == "amf_message_history"{
		Query := INSERT_INTO_MESSAGE_HISTORY+SELECT_MESSAGES+" where create_time <= $1"
		resp, err := Db.Exec(Query,then.Format("2006-01-02 15:04:05"))
		fmt.Printf("response for adding data to %v is: %v\n",tablename,resp)
		if err != nil{
			return err
		}
	} else if tablename == "amf_session_history"{
		Query := INSERT_INTO_SESSION_HISTORY+SELECT_SESSIONS+" where create_time <= $1"
		resp, err := Db.Exec(Query,then.Format("2006-01-02 15:04:05"))
		fmt.Printf("response for adding data to %v is: %v\n",tablename,resp)
		if err != nil{
			return err
		}
		return nil
	} else if tablename == "amf_session_rel_history"{
		Query := INSERT_INTO_SESSION_REL_HISTORY+SELECT_SESSION_REL+" where create_time <= $1"
		resp, err := Db.Exec(Query,then.Format("2006-01-02 15:04:05"))
		fmt.Printf("response for adding data to %v is: %v\n",tablename,resp)
		if err != nil{
			return err
		}
		return nil
	} else if tablename == "amf_event_history"{
		Query := INSERT_INTO_EVENT_HISTORY+SELECT_EVENT+" where create_time <= $1"
		resp, err := Db.Exec(Query,then.Format("2006-01-02 15:04:05"))
		fmt.Printf("response for adding data to %v is: %v\n",tablename,resp)
		if err != nil{
			return err
		}
		return nil
	}
	return nil
}


func (util *DbUtil) InsertLastMonthHistory(Db *sql.DB,firstOfMonth,lastOfMonth,tablename string) error{
	if tablename == "amf_message_history"{
		whereClause := " where create_time >= '"+firstOfMonth+"' and create_time <= '"+lastOfMonth+"'"
		Query := INSERT_INTO_MESSAGE_HISTORY+SELECT_MESSAGES+whereClause
		resp, err := Db.Exec(Query)
		fmt.Printf("response for adding data to %v is: %v\n",tablename,resp)
		if err != nil{
			return err
		}
		return nil
	} else if tablename == "amf_session_history"{
		whereClause := " where create_time >= '"+firstOfMonth+"' and create_time <= '"+lastOfMonth+"'"
		Query := INSERT_INTO_SESSION_HISTORY+SELECT_SESSIONS+whereClause
		resp, err := Db.Exec(Query)
		fmt.Printf("response for adding data to %v is: %v\n",tablename,resp)
		if err != nil{
			return err
		}
		return nil
	} else if tablename == "amf_session_rel_history"{
		whereClause := " where create_time >= '"+firstOfMonth+"' and create_time <= '"+lastOfMonth+"'"
		Query := INSERT_INTO_SESSION_REL_HISTORY+SELECT_SESSION_REL+whereClause
		resp, err := Db.Exec(Query)
		fmt.Printf("response for adding data to %v is: %v\n",tablename,resp)
		if err != nil{
			return err
		}
		return nil
	} else if tablename == "amf_event_history"{
		whereClause := " where create_time >= '"+firstOfMonth+"' and create_time <= '"+lastOfMonth+"'"
		Query := INSERT_INTO_EVENT_HISTORY+SELECT_EVENT+whereClause
		resp, err := Db.Exec(Query)
		fmt.Printf("response for adding data to %v is: %v\n",tablename,resp)
		if err != nil{
			return err
		}
		return nil
	}
	return nil
}

