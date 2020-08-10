package com.atguigu.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class MyUDTF2 extends GenericUDTF {

    private List<String> outList = new ArrayList<>();

    /**
     * 约定函数输出的列的名字  和 列的类型。
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     *
     * my
     */

    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //约定函数输出的列的名字
        ArrayList<String>  fieldNames  = new ArrayList<>() ;
        // 约定函数输出的列的类型
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();

        fieldNames.add("word1");
        fieldNames.add("word2");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    /**select myudtf2("hello_world,Hadoop_hive",",","_")
     * 函数的处理逻辑
     * @param args
     * @throws HiveException
     */

    public void process(Object[] args) throws HiveException {
        //简单判断
        if(args==null || args.length <3){
            throw new HiveException("Input Args Length Error");
        }
        //获取待处理的数据
        String argsData = args[0].toString();
        //获取分隔符1
        String argsSplit1 = args[1].toString();
        String argsSplit2 = args[2].toString();
        //切割数据
        String[] words = argsData.split(argsSplit1);

        //迭代写出
        for (String word : words) {

            String[] currentWords= word.split(argsSplit2);
            //因为集合是复用的， 使用前先清空之前数据
            outList.clear();

            for (String currentWord : currentWords) {
                outList.add(currentWord);
            }
            //写出

            forward(outList);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
