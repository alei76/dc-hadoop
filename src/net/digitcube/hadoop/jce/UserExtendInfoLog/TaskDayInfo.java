// **********************************************************************
// This file was generated by a TAF parser!
// TAF version 3.0.0.25 by WSRD Tencent.
// Generated from `item.jce'
// **********************************************************************

package net.digitcube.hadoop.jce.UserExtendInfoLog;

import net.digitcube.protocol.JceDisplayer;
import net.digitcube.protocol.JceInputStream;
import net.digitcube.protocol.JceOutputStream;
import net.digitcube.protocol.JceStruct;
import net.digitcube.protocol.JceUtil;

public final class TaskDayInfo extends JceStruct implements java.lang.Cloneable
{
    public String className()
    {
        return "UserExtendInfoLog.TaskDayInfo";
    }

    public String fullClassName()
    {
        return "net.digitcube.hadoop.jce.UserExtendInfoLog.TaskDayInfo";
    }

    public String taskId = "";

    public String taskType = "";

    public int beginTimes = 0;

    public int successTimes = 0;

    public int failueTimes = 0;

    public java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount> itemBuySet = null;

    public java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount> itemUseSet = null;

    public java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount> itemGetSet = null;

    public String getTaskId()
    {
        return taskId;
    }

    public void  setTaskId(String taskId)
    {
        this.taskId = taskId;
    }

    public String getTaskType()
    {
        return taskType;
    }

    public void  setTaskType(String taskType)
    {
        this.taskType = taskType;
    }

    public int getBeginTimes()
    {
        return beginTimes;
    }

    public void  setBeginTimes(int beginTimes)
    {
        this.beginTimes = beginTimes;
    }

    public int getSuccessTimes()
    {
        return successTimes;
    }

    public void  setSuccessTimes(int successTimes)
    {
        this.successTimes = successTimes;
    }

    public int getFailueTimes()
    {
        return failueTimes;
    }

    public void  setFailueTimes(int failueTimes)
    {
        this.failueTimes = failueTimes;
    }

    public java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount> getItemBuySet()
    {
        return itemBuySet;
    }

    public void  setItemBuySet(java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount> itemBuySet)
    {
        this.itemBuySet = itemBuySet;
    }

    public java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount> getItemUseSet()
    {
        return itemUseSet;
    }

    public void  setItemUseSet(java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount> itemUseSet)
    {
        this.itemUseSet = itemUseSet;
    }

    public java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount> getItemGetSet()
    {
        return itemGetSet;
    }

    public void  setItemGetSet(java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount> itemGetSet)
    {
        this.itemGetSet = itemGetSet;
    }

    public TaskDayInfo()
    {
        setTaskId(taskId);
        setTaskType(taskType);
        setBeginTimes(beginTimes);
        setSuccessTimes(successTimes);
        setFailueTimes(failueTimes);
        setItemBuySet(itemBuySet);
        setItemUseSet(itemUseSet);
        setItemGetSet(itemGetSet);
    }

    public TaskDayInfo(String taskId, String taskType, int beginTimes, int successTimes, int failueTimes, java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount> itemBuySet, java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount> itemUseSet, java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount> itemGetSet)
    {
        setTaskId(taskId);
        setTaskType(taskType);
        setBeginTimes(beginTimes);
        setSuccessTimes(successTimes);
        setFailueTimes(failueTimes);
        setItemBuySet(itemBuySet);
        setItemUseSet(itemUseSet);
        setItemGetSet(itemGetSet);
    }

    public boolean equals(Object o)
    {
        if(o == null)
        {
            return false;
        }

        TaskDayInfo t = (TaskDayInfo) o;
        return (
            JceUtil.equals(taskId, t.taskId) && 
            JceUtil.equals(taskType, t.taskType) && 
            JceUtil.equals(beginTimes, t.beginTimes) && 
            JceUtil.equals(successTimes, t.successTimes) && 
            JceUtil.equals(failueTimes, t.failueTimes) && 
            JceUtil.equals(itemBuySet, t.itemBuySet) && 
            JceUtil.equals(itemUseSet, t.itemUseSet) && 
            JceUtil.equals(itemGetSet, t.itemGetSet) );
    }

    public int hashCode()
    {
        try
        {
            throw new Exception("Need define key first!");
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
        return 0;
    }
    public java.lang.Object clone()
    {
        java.lang.Object o = null;
        try
        {
            o = super.clone();
        }
        catch(CloneNotSupportedException ex)
        {
            assert false; // impossible
        }
        return o;
    }

    public void writeTo(JceOutputStream _os)
    {
        _os.write(taskId, 0);
        if (null != taskType)
        {
            _os.write(taskType, 1);
        }
        _os.write(beginTimes, 2);
        _os.write(successTimes, 3);
        _os.write(failueTimes, 4);
        if (null != itemBuySet)
        {
            _os.write(itemBuySet, 5);
        }
        if (null != itemUseSet)
        {
            _os.write(itemUseSet, 6);
        }
        if (null != itemGetSet)
        {
            _os.write(itemGetSet, 7);
        }
    }

    static java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount> cache_itemBuySet;
    static java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount> cache_itemUseSet;
    static java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount> cache_itemGetSet;

    public void readFrom(JceInputStream _is)
    {
        setTaskId( _is.readString(0, true));

        setTaskType( _is.readString(1, false));

        setBeginTimes((int) _is.read(beginTimes, 2, false));

        setSuccessTimes((int) _is.read(successTimes, 3, false));

        setFailueTimes((int) _is.read(failueTimes, 4, false));

        if(null == cache_itemBuySet)
        {
            cache_itemBuySet = new java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount>();
            net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount __var_4 = new net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount();
            ((java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount>)cache_itemBuySet).add(__var_4);
        }
        setItemBuySet((java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount>) _is.read(cache_itemBuySet, 5, false));

        if(null == cache_itemUseSet)
        {
            cache_itemUseSet = new java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount>();
            net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount __var_5 = new net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount();
            ((java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount>)cache_itemUseSet).add(__var_5);
        }
        setItemUseSet((java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount>) _is.read(cache_itemUseSet, 6, false));

        if(null == cache_itemGetSet)
        {
            cache_itemGetSet = new java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount>();
            net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount __var_6 = new net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount();
            ((java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount>)cache_itemGetSet).add(__var_6);
        }
        setItemGetSet((java.util.ArrayList<net.digitcube.hadoop.jce.UserExtendInfoLog.ItemCount>) _is.read(cache_itemGetSet, 7, false));

    }

    public void display(java.lang.StringBuilder _os, int _level)
    {
        JceDisplayer _ds = new JceDisplayer(_os, _level);
        _ds.display(taskId, "taskId");
        _ds.display(taskType, "taskType");
        _ds.display(beginTimes, "beginTimes");
        _ds.display(successTimes, "successTimes");
        _ds.display(failueTimes, "failueTimes");
        _ds.display(itemBuySet, "itemBuySet");
        _ds.display(itemUseSet, "itemUseSet");
        _ds.display(itemGetSet, "itemGetSet");
    }

}

