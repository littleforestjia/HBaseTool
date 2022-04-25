package com.jd.hbase.log;

import java.util.Date;


//用于监控和记录hbase推数过程中花费的时间、成功率、数据量等。

/**
 *	 导数过程的监控类。花费时间，成功失败个数，数据量等。
 *
 */
public class PickerMonitor {

	static class TimeLog {
		public TimeLog() {
			super();
		}

		long lastTime;
		long timeSpan;

		//记录和计算时间使用的单位是纳秒，先转化成分、秒格式
		//measure with nanosecond
		public String getMeasureTimeDesc() {
			if (timeSpan / 60000000000l > 0)
				return (int) ((timeSpan) / 60000000000l) + " m "
						+ (double) (timeSpan % 60000000000l) / 1000000000
						+ " s ";
			if (timeSpan / 1000000000 > 0)
				return (int) ((timeSpan) / 1000000000) + " s "
						+ (double) (timeSpan % 1000000000) / 1000000 + " ms ";
			if (timeSpan / 1000000 > 0)
				return (double) (timeSpan) / 1000000 + " ms ";
			else
				return (double) (timeSpan) / 1000 + " microsecond ";
		}

		public void startMeasureTime() {
			lastTime = System.nanoTime();
		}

		public void stopMeasureTime() {
			long timeTmp = System.nanoTime();
			long timeSpan = timeTmp - lastTime;
			lastTime = timeTmp;
			this.timeSpan += timeSpan;
		}

		public long getTimeSpan() {
			return timeSpan;
		}
	}

	private static TimeLog[] timeLogArray = new TimeLog[4];

	private static Date startTime, stopTime;

	private static String srcTable, destTable, sysLogError, srcPath, srcArgs;

	private static long srcTotal, srcSucc, destTotal, destSucc, putSize,
			skipCount;

	public static void init() {
		startTime = null;
		stopTime = null;
		srcTable = null;
		destTable = null;
		sysLogError = null;
		srcPath = null;
		srcArgs = null;
		srcTotal = 0;
		srcSucc = 0;
		skipCount = 0;
		destTotal = 0;
		destSucc = 0;
		putSize = 0;
	}

	public static Date getStartTime() {
		return startTime;
	}

	public static void setStartTime(Date startTime) {
		PickerMonitor.startTime = startTime;
	}

	public static Date getStopTime() {
		return stopTime;
	}

	public static void setStopTime(Date stopTime) {
		PickerMonitor.stopTime = stopTime;
	}

	public static String getSrcTable() {
		return srcTable;
	}

	public static void setSrcTable(String srcTable) {
		PickerMonitor.srcTable = srcTable;
	}

	public static String getDestTable() {
		return destTable;
	}

	public static void setDestTable(String destTable) {
		PickerMonitor.destTable = destTable;
	}

	public static String getSysLogError() {
		return sysLogError;
	}

	public static void setSysLogError(String sysLogError) {
		PickerMonitor.sysLogError = sysLogError;
	}

	public static long getPutSize() {
		return putSize;
	}

	public static void setPutSize(long putSize) {
		PickerMonitor.putSize = putSize;
	}

	public static long getDestTotal() {
		return destTotal;
	}

	public static void setDestTotal(long destTotal) {
		PickerMonitor.destTotal = destTotal;
	}

	public static long getDestSucc() {
		return destSucc;
	}

	public static void setDestSucc(long destSucc) {
		PickerMonitor.destSucc = destSucc;
	}

	public static long getSrcSucc() {
		return srcSucc;
	}

	public static void setSrcSucc(long srcSucc) {
		PickerMonitor.srcSucc = srcSucc;
	}

	public static long getSrcTotal() {
		return srcTotal;
	}

	public static String getSrcPath() {
		return srcPath;
	}

	public static void setSrcPath(String srcPath) {
		PickerMonitor.srcPath = srcPath;
	}

	public static String getSrcArgs() {
		return srcArgs;
	}

	public static void setSrcArgs(String srcArgs) {
		PickerMonitor.srcArgs = srcArgs;
	}

	public static void setSrcTotal(long srcTotal) {
		PickerMonitor.srcTotal = srcTotal;
	}

	static {
		for (int i = 0; i < timeLogArray.length; i++) {
			timeLogArray[i] = new TimeLog();
		}
	}

	public static void startMeasureTime(int index) {
		timeLogArray[index].startMeasureTime();
	}

	public static void stopMeasureTime(int index) {
		timeLogArray[index].stopMeasureTime();
	}

	public static String getMeasureTimeDesc(int index) {
		return timeLogArray[index].getMeasureTimeDesc();
	}

	public static void showTestTime() {
		for (int i = 0; i < timeLogArray.length; i++) {
			System.out.println(i + ": " + timeLogArray[i].getMeasureTimeDesc());
		}
	}

	public static void showMonitor() {
		System.out.println("startTime: " + startTime);
		System.out.println("stopTime: " + stopTime);
		System.out.println("srcTable: " + srcTable);
		System.out.println("destTable: " + destTable);
		System.out.println("srcTotal: " + srcTotal);
		System.out.println("srcSucc: " + srcSucc);
		System.out.println("destTotal: " + destTotal);
		System.out.println("skipCount: " + skipCount);
		System.out.println("destSucc: " + destSucc);
		System.out.println("putSize: " + putSize);
		System.out.println("sysLogError: " + sysLogError);
		System.out.println("srcPath: " + srcPath);
		System.out.println("srcArgs: " + srcArgs);
	}

	public static long getSkipCount() {
		return skipCount;
	}

	public static void incSkipCount() {
		PickerMonitor.skipCount++;
	}

}
