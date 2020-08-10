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

/**
 * 将指定的字符串通过指定的分隔符，进行拆分，返回多行数据
 *
 * 自定义UDTF,需要继承GenericUDTF
 */
public class MyUDTFT extends GenericUDTF {


    private List<String> outList = new ArrayList<>();

    /**
     * 约定函数输出的列的名字  和 列的类型。
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //约定函数输出的列的名字
        ArrayList<String>  fieldNames  = new ArrayList<>() ;
        // 约定函数输出的列的类型
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();

        fieldNames.add("word");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    /**
     * 函数的处理逻辑
     * @param args
     * @throws HiveException
     */
    @Override
    public void process(Object[] args) throws HiveException {
        //简单判断
        if(args==null || args.length <2){
            throw new HiveException("Input Args Length Error");
        }
        //获取待处理的数据
        String argsData = args[0].toString();
        //获取分隔符
        String argsSplit = args[1].toString();

        //切割数据
        String[] words = argsData.split(argsSplit);

        //迭代写出
        for (String word : words) {
            //因为集合是复用的， 使用前先清空之前数据
            outList.clear();
            //写出
            outList.add(word);
            forward(outList);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
