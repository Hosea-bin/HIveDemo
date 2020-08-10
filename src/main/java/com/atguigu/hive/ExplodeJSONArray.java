package com.atguigu.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

public class ExplodeJSONArray extends GenericUDTF {

    //声明炸开数据的列名和类型
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //列名集合
        List<String> fieldNames=new ArrayList<>();
        fieldNames.add("action");
        //列类型校验器的集合
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        //返回
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }
    //遍历一行数据，做炸开操作【多次写出操作】
    @Override
    public void process(Object[] args) throws HiveException {
        //判断是否为空
        if(args[0] == null){
            return;
        }
        //获取UDTF函数的输入数据
        String input = args[0].toString();

        //对输入的数据创建JSON数组
        JSONArray jsonArray = new JSONArray(input);
        //遍历JSON数组，将每个元素写出
        for (int i = 0; i <jsonArray.length() ; i++) {
            ArrayList<Object> objects = new ArrayList<>();
            objects.clear();
            objects.add(jsonArray.getString(i));
            forward(objects);
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
