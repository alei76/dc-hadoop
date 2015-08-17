package net.digitcube.hadoop.mapreduce.html5.html5new.vo;


import net.digitcube.protocol.JceDisplayer;
import net.digitcube.protocol.JceInputStream;
import net.digitcube.protocol.JceOutputStream;
import net.digitcube.protocol.JceStruct;
import net.digitcube.protocol.JceUtil;

public final class H5PlayerDayInfo extends JceStruct implements java.lang.Cloneable
{
    public String className()
    {
        return "PlayerInfo.H5PlayerDayInfo";
    }

    public String fullClassName()
    {
        return "H5PlayerDayInfo";
    }

    public String uid = "";

    public int platform = 0;

    public String parentId = "";

    public String promptApp = "";

    public String refer = "";

    public String domain = "";

    public int createTime = 0;

    public int firstLoginDate = 0;

    public int lastLoginDate = 0;

    public int firstPayDate = 0;

    public int lastPayDate = 0;

    public int totalOnlineDay = 0;

    public int totalLoginTimes = 0;

    public int totalOnlineTime = 0;

    public int totalPayTimes = 0;

    public int totalCurrencyAmount = 0;

    public int track = 0;

    public int payTrack = 0;

    public java.util.ArrayList<H5OnlineDay> onlineDayList = null;

    public String getUid()
    {
        return uid;
    }

    public void  setUid(String uid)
    {
        this.uid = uid;
    }

    public int getPlatform()
    {
        return platform;
    }

    public void  setPlatform(int platform)
    {
        this.platform = platform;
    }

    public String getParentId()
    {
        return parentId;
    }

    public void  setParentId(String parentId)
    {
        this.parentId = parentId;
    }

    public String getPromptApp()
    {
        return promptApp;
    }

    public void  setPromptApp(String promptApp)
    {
        this.promptApp = promptApp;
    }

    public String getRefer()
    {
        return refer;
    }

    public void  setRefer(String refer)
    {
        this.refer = refer;
    }

    public String getDomain()
    {
        return domain;
    }

    public void  setDomain(String domain)
    {
        this.domain = domain;
    }

    public int getCreateTime()
    {
        return createTime;
    }

    public void  setCreateTime(int createTime)
    {
        this.createTime = createTime;
    }

    public int getFirstLoginDate()
    {
        return firstLoginDate;
    }

    public void  setFirstLoginDate(int firstLoginDate)
    {
        this.firstLoginDate = firstLoginDate;
    }

    public int getLastLoginDate()
    {
        return lastLoginDate;
    }

    public void  setLastLoginDate(int lastLoginDate)
    {
        this.lastLoginDate = lastLoginDate;
    }

    public int getFirstPayDate()
    {
        return firstPayDate;
    }

    public void  setFirstPayDate(int firstPayDate)
    {
        this.firstPayDate = firstPayDate;
    }

    public int getLastPayDate()
    {
        return lastPayDate;
    }

    public void  setLastPayDate(int lastPayDate)
    {
        this.lastPayDate = lastPayDate;
    }

    public int getTotalOnlineDay()
    {
        return totalOnlineDay;
    }

    public void  setTotalOnlineDay(int totalOnlineDay)
    {
        this.totalOnlineDay = totalOnlineDay;
    }

    public int getTotalLoginTimes()
    {
        return totalLoginTimes;
    }

    public void  setTotalLoginTimes(int totalLoginTimes)
    {
        this.totalLoginTimes = totalLoginTimes;
    }

    public int getTotalOnlineTime()
    {
        return totalOnlineTime;
    }

    public void  setTotalOnlineTime(int totalOnlineTime)
    {
        this.totalOnlineTime = totalOnlineTime;
    }

    public int getTotalPayTimes()
    {
        return totalPayTimes;
    }

    public void  setTotalPayTimes(int totalPayTimes)
    {
        this.totalPayTimes = totalPayTimes;
    }

    public int getTotalCurrencyAmount()
    {
        return totalCurrencyAmount;
    }

    public void  setTotalCurrencyAmount(int totalCurrencyAmount)
    {
        this.totalCurrencyAmount = totalCurrencyAmount;
    }

    public int getTrack()
    {
        return track;
    }

    public void  setTrack(int track)
    {
        this.track = track;
    }

    public int getPayTrack()
    {
        return payTrack;
    }

    public void  setPayTrack(int payTrack)
    {
        this.payTrack = payTrack;
    }

    public java.util.ArrayList<H5OnlineDay> getOnlineDayList()
    {
        return onlineDayList;
    }

    public void  setOnlineDayList(java.util.ArrayList<H5OnlineDay> onlineDayList)
    {
        this.onlineDayList = onlineDayList;
    }

    public H5PlayerDayInfo()
    {
        setUid(uid);
        setPlatform(platform);
        setParentId(parentId);
        setPromptApp(promptApp);
        setRefer(refer);
        setDomain(domain);
        setCreateTime(createTime);
        setFirstLoginDate(firstLoginDate);
        setLastLoginDate(lastLoginDate);
        setFirstPayDate(firstPayDate);
        setLastPayDate(lastPayDate);
        setTotalOnlineDay(totalOnlineDay);
        setTotalLoginTimes(totalLoginTimes);
        setTotalOnlineTime(totalOnlineTime);
        setTotalPayTimes(totalPayTimes);
        setTotalCurrencyAmount(totalCurrencyAmount);
        setTrack(track);
        setPayTrack(payTrack);
        setOnlineDayList(onlineDayList);
    }

    public H5PlayerDayInfo(String uid, int platform, String parentId, String promptApp, String refer, String domain, int createTime, int firstLoginDate, int lastLoginDate, int firstPayDate, int lastPayDate, int totalOnlineDay, int totalLoginTimes, int totalOnlineTime, int totalPayTimes, int totalCurrencyAmount, int track, int payTrack, java.util.ArrayList<H5OnlineDay> onlineDayList)
    {
        setUid(uid);
        setPlatform(platform);
        setParentId(parentId);
        setPromptApp(promptApp);
        setRefer(refer);
        setDomain(domain);
        setCreateTime(createTime);
        setFirstLoginDate(firstLoginDate);
        setLastLoginDate(lastLoginDate);
        setFirstPayDate(firstPayDate);
        setLastPayDate(lastPayDate);
        setTotalOnlineDay(totalOnlineDay);
        setTotalLoginTimes(totalLoginTimes);
        setTotalOnlineTime(totalOnlineTime);
        setTotalPayTimes(totalPayTimes);
        setTotalCurrencyAmount(totalCurrencyAmount);
        setTrack(track);
        setPayTrack(payTrack);
        setOnlineDayList(onlineDayList);
    }

    public boolean equals(Object o)
    {
        if(o == null)
        {
            return false;
        }

        H5PlayerDayInfo t = (H5PlayerDayInfo) o;
        return (
            JceUtil.equals(uid, t.uid) && 
            JceUtil.equals(platform, t.platform) && 
            JceUtil.equals(parentId, t.parentId) && 
            JceUtil.equals(promptApp, t.promptApp) && 
            JceUtil.equals(refer, t.refer) && 
            JceUtil.equals(domain, t.domain) && 
            JceUtil.equals(createTime, t.createTime) && 
            JceUtil.equals(firstLoginDate, t.firstLoginDate) && 
            JceUtil.equals(lastLoginDate, t.lastLoginDate) && 
            JceUtil.equals(firstPayDate, t.firstPayDate) && 
            JceUtil.equals(lastPayDate, t.lastPayDate) && 
            JceUtil.equals(totalOnlineDay, t.totalOnlineDay) && 
            JceUtil.equals(totalLoginTimes, t.totalLoginTimes) && 
            JceUtil.equals(totalOnlineTime, t.totalOnlineTime) && 
            JceUtil.equals(totalPayTimes, t.totalPayTimes) && 
            JceUtil.equals(totalCurrencyAmount, t.totalCurrencyAmount) && 
            JceUtil.equals(track, t.track) && 
            JceUtil.equals(payTrack, t.payTrack) && 
            JceUtil.equals(onlineDayList, t.onlineDayList) );
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
        if (null != uid)
        {
            _os.write(uid, 0);
        }
        _os.write(platform, 1);
        if (null != parentId)
        {
            _os.write(parentId, 2);
        }
        if (null != promptApp)
        {
            _os.write(promptApp, 3);
        }
        if (null != refer)
        {
            _os.write(refer, 4);
        }
        if (null != domain)
        {
            _os.write(domain, 5);
        }
        _os.write(createTime, 6);
        _os.write(firstLoginDate, 7);
        _os.write(lastLoginDate, 8);
        _os.write(firstPayDate, 9);
        _os.write(lastPayDate, 10);
        _os.write(totalOnlineDay, 11);
        _os.write(totalLoginTimes, 12);
        _os.write(totalOnlineTime, 13);
        _os.write(totalPayTimes, 14);
        _os.write(totalCurrencyAmount, 15);
        _os.write(track, 16);
        _os.write(payTrack, 17);
        if (null != onlineDayList)
        {
            _os.write(onlineDayList, 18);
        }
    }

    static java.util.ArrayList<H5OnlineDay> cache_onlineDayList;

    public void readFrom(JceInputStream _is)
    {
        setUid( _is.readString(0, false));

        setPlatform((int) _is.read(platform, 1, false));

        setParentId( _is.readString(2, false));

        setPromptApp( _is.readString(3, false));

        setRefer( _is.readString(4, false));

        setDomain( _is.readString(5, false));

        setCreateTime((int) _is.read(createTime, 6, false));

        setFirstLoginDate((int) _is.read(firstLoginDate, 7, false));

        setLastLoginDate((int) _is.read(lastLoginDate, 8, false));

        setFirstPayDate((int) _is.read(firstPayDate, 9, false));

        setLastPayDate((int) _is.read(lastPayDate, 10, false));

        setTotalOnlineDay((int) _is.read(totalOnlineDay, 11, false));

        setTotalLoginTimes((int) _is.read(totalLoginTimes, 12, false));

        setTotalOnlineTime((int) _is.read(totalOnlineTime, 13, false));

        setTotalPayTimes((int) _is.read(totalPayTimes, 14, false));

        setTotalCurrencyAmount((int) _is.read(totalCurrencyAmount, 15, false));

        setTrack((int) _is.read(track, 16, false));

        setPayTrack((int) _is.read(payTrack, 17, false));

        if(null == cache_onlineDayList)
        {
            cache_onlineDayList = new java.util.ArrayList<H5OnlineDay>();
            H5OnlineDay __var_9 = new H5OnlineDay();
            ((java.util.ArrayList<H5OnlineDay>)cache_onlineDayList).add(__var_9);
        }
        setOnlineDayList((java.util.ArrayList<H5OnlineDay>) _is.read(cache_onlineDayList, 18, false));

    }

    public void display(java.lang.StringBuilder _os, int _level)
    {
        JceDisplayer _ds = new JceDisplayer(_os, _level);
        _ds.display(uid, "uid");
        _ds.display(platform, "platform");
        _ds.display(parentId, "parentId");
        _ds.display(promptApp, "promptApp");
        _ds.display(refer, "refer");
        _ds.display(domain, "domain");
        _ds.display(createTime, "createTime");
        _ds.display(firstLoginDate, "firstLoginDate");
        _ds.display(lastLoginDate, "lastLoginDate");
        _ds.display(firstPayDate, "firstPayDate");
        _ds.display(lastPayDate, "lastPayDate");
        _ds.display(totalOnlineDay, "totalOnlineDay");
        _ds.display(totalLoginTimes, "totalLoginTimes");
        _ds.display(totalOnlineTime, "totalOnlineTime");
        _ds.display(totalPayTimes, "totalPayTimes");
        _ds.display(totalCurrencyAmount, "totalCurrencyAmount");
        _ds.display(track, "track");
        _ds.display(payTrack, "payTrack");
        _ds.display(onlineDayList, "onlineDayList");
    }

}


