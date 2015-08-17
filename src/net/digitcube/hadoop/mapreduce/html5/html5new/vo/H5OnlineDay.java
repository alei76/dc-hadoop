package net.digitcube.hadoop.mapreduce.html5.html5new.vo;

import net.digitcube.protocol.JceDisplayer;
import net.digitcube.protocol.JceInputStream;
import net.digitcube.protocol.JceOutputStream;
import net.digitcube.protocol.JceStruct;
import net.digitcube.protocol.JceUtil;

public class H5OnlineDay extends JceStruct implements java.lang.Cloneable {
	
	    public String className()
	    {
	        return "PlayerInfo.H5OnlineDay";
	    }

	    public String fullClassName()
	    {
	        return "net.digitcube.hadoop.mapreduce.html5.html5new.vo.PlayerInfo.H5OnlineDay";
	    }

	    public int onlineDate = 0;

	    public int onlineTime = 0;

	    public int loginTimes = 0;

	    public int payAmount = 0;

	    public int payTimes = 0;

	    public int maxLevel = 0;

	    public int getOnlineDate()
	    {
	        return onlineDate;
	    }

	    public void  setOnlineDate(int onlineDate)
	    {
	        this.onlineDate = onlineDate;
	    }

	    public int getOnlineTime()
	    {
	        return onlineTime;
	    }

	    public void  setOnlineTime(int onlineTime)
	    {
	        this.onlineTime = onlineTime;
	    }

	    public int getLoginTimes()
	    {
	        return loginTimes;
	    }

	    public void  setLoginTimes(int loginTimes)
	    {
	        this.loginTimes = loginTimes;
	    }

	    public int getPayAmount()
	    {
	        return payAmount;
	    }

	    public void  setPayAmount(int payAmount)
	    {
	        this.payAmount = payAmount;
	    }

	    public int getPayTimes()
	    {
	        return payTimes;
	    }

	    public void  setPayTimes(int payTimes)
	    {
	        this.payTimes = payTimes;
	    }

	    public int getMaxLevel()
	    {
	        return maxLevel;
	    }

	    public void  setMaxLevel(int maxLevel)
	    {
	        this.maxLevel = maxLevel;
	    }

	    public H5OnlineDay()
	    {
	        setOnlineDate(onlineDate);
	        setOnlineTime(onlineTime);
	        setLoginTimes(loginTimes);
	        setPayAmount(payAmount);
	        setPayTimes(payTimes);
	        setMaxLevel(maxLevel);
	    }

	    public H5OnlineDay(int onlineDate, int onlineTime, int loginTimes, int payAmount, int payTimes, int maxLevel)
	    {
	        setOnlineDate(onlineDate);
	        setOnlineTime(onlineTime);
	        setLoginTimes(loginTimes);
	        setPayAmount(payAmount);
	        setPayTimes(payTimes);
	        setMaxLevel(maxLevel);
	    }

	    public boolean equals(Object o)
	    {
	        if(o == null)
	        {
	            return false;
	        }

	        H5OnlineDay t = (H5OnlineDay) o;
	        return (
	            JceUtil.equals(onlineDate, t.onlineDate) && 
	            JceUtil.equals(onlineTime, t.onlineTime) && 
	            JceUtil.equals(loginTimes, t.loginTimes) && 
	            JceUtil.equals(payAmount, t.payAmount) && 
	            JceUtil.equals(payTimes, t.payTimes) && 
	            JceUtil.equals(maxLevel, t.maxLevel) );
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
	        _os.write(onlineDate, 0);
	        _os.write(onlineTime, 1);
	        _os.write(loginTimes, 2);
	        _os.write(payAmount, 3);
	        _os.write(payTimes, 4);
	        _os.write(maxLevel, 5);
	    }


	    public void readFrom(JceInputStream _is)
	    {
	        setOnlineDate((int) _is.read(onlineDate, 0, false));

	        setOnlineTime((int) _is.read(onlineTime, 1, false));

	        setLoginTimes((int) _is.read(loginTimes, 2, false));

	        setPayAmount((int) _is.read(payAmount, 3, false));

	        setPayTimes((int) _is.read(payTimes, 4, false));

	        setMaxLevel((int) _is.read(maxLevel, 5, false));

	    }

	    public void display(java.lang.StringBuilder _os, int _level)
	    {
	        JceDisplayer _ds = new JceDisplayer(_os, _level);
	        _ds.display(onlineDate, "onlineDate");
	        _ds.display(onlineTime, "onlineTime");
	        _ds.display(loginTimes, "loginTimes");
	        _ds.display(payAmount, "payAmount");
	        _ds.display(payTimes, "payTimes");
	        _ds.display(maxLevel, "maxLevel");
	    }

}
