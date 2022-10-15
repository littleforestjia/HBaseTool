package com.jd.hbase.test;

import com.jd.hbase.bulkloadReappear.BulkloadMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;

public class TestClass {

    //重要：单元测试发现，所有大小的int类型数据转化成字节数组后，字节数组长度都是4。
    @Test
    public void test1() {
        int int1 = 1;
        int int2 = 12345;
        int int3 = Integer.reverse(int1);
        int int4 = Integer.reverse(int2);
        byte[] bytes1 = Bytes.toBytes(int1);
        byte[] bytes2 = Bytes.toBytes(int2);
        byte[] bytes3 = Bytes.toBytes(int3);
        byte[] bytes4 = Bytes.toBytes(int4);

        System.out.println(bytes1.length);
        System.out.println(bytes2.length);
        System.out.println(bytes3.length);
        System.out.println(bytes4.length);
    }

    @Test
    public void test2() {
        long long1 = 1;
        long long2 = Long.reverse(long1);

        byte[] bytes1 = Bytes.toBytes(long1);
        byte[] bytes2 = Bytes.toBytes(long2);

        System.out.println(long1);
        System.out.println(bytes1.length);
        System.out.println(long2);
        System.out.println(bytes2.length);
    }

    //HBase表数据测试
    @Test
    public void test3() {

        int reverseId = Integer.reverse(37438);
        byte[] idByte = Bytes.toBytes(reverseId);
        System.out.println(BinaryToHexString(idByte));

    }

    //字节数据拼接测试
    @Test
    public void test4() {
        int reverseId = Integer.reverse(37438);
        byte[] idByte = Bytes.toBytes(reverseId);
        byte[] birthdayByte = Bytes.toBytes("2022/4/11");

        //将两个byte[]拼接在一起。
        int length = idByte.length + birthdayByte.length;
        byte[] newBytes = new byte[length];
        System.arraycopy(idByte, 0, newBytes, 0, 4);
        System.arraycopy(birthdayByte, 0, newBytes, 4, birthdayByte.length);
        System.out.println(BinaryToHexString(newBytes));
    }

    /**
     *
     * @param bytes
     * @return 将二进制转换为十六进制字符输出
     */
    private static String hexStr = "0123456789ABCDEF"; //全局
    public static String BinaryToHexString(byte[] bytes){

        String result = "";
        String hex = "";
        for(int i=0;i<bytes.length;i++){
            //字节高4位
            hex = String.valueOf(hexStr.charAt((bytes[i]&0xF0)>>4));
            //字节低4位
            hex += String.valueOf(hexStr.charAt(bytes[i]&0x0F));
            result +=hex;
        }
        return result;
    }
    /**
     *
     * @param hexString
     * @return 将十六进制转换为字节数组
     */
    public static byte[] HexStringToBinary(String hexString){
        //hexString的长度对2取整，作为bytes的长度
        int len = hexString.length()/2;
        byte[] bytes = new byte[len];
        byte high = 0;//字节高四位
        byte low = 0;//字节低四位

        for(int i=0;i<len;i++){
            //右移四位得到高位
            high = (byte)((hexStr.indexOf(hexString.charAt(2*i)))<<4);
            low = (byte)hexStr.indexOf(hexString.charAt(2*i+1));
            bytes[i] = (byte) (high|low);//高地位做或运算
        }
        return bytes;
    }

     public enum A {

         HAHA("有"),
         XIXI("无");

         private String value;

        A(String value) {
            this.value = value;
        }
     }

    @Test
    public void test5() {
        A str = A.values()[0];
        System.out.println(str.value);
        A str1 = A.values()[2];
        System.out.println(str1.value);
    }


    @Test
    public void test6() {
        int a = 1;
        long b = 1;
        double c = 1;

        byte[] bytesa = Bytes.toBytes(a);
        byte[] bytesb = Bytes.toBytes(b);
        byte[] bytesc = Bytes.toBytes(c);

        System.out.println(bytesa.length);
        System.out.println(bytesb.length);
        System.out.println(bytesc.length);
    }
}

