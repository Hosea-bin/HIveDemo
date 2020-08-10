package com.atguigu.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 需求：计算传入的字符串的长度，
 */
public class MyStringLength extends GenericUDF {
    /**
     * 对输入参数的判断处理，及返回类型的约定
     * @param objectInspectors 传入到函数的参数的类型对应的ObjectInspector
     * @return
     * @throws UDFArgumentException
     */
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if(objectInspectors.length!=1||objectInspectors==null){
            throw new UDFArgumentLengthException("Input Args Length Error!!!");
        }
        if(!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentTypeException(0,"Input Args Type Error!!!");
        }
        //约定函数的返回值为int
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 函数逻辑数据
     * @param deferredObjects 传入函数的参数
     * @return
     * @throws HiveException
     */
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        //取出参数
        Object o = deferredObjects[0].get();
        if(o==null){
            return 0;
        }
        return o.toString().length();
    }

    public String getDisplayString(String[] strings) {
        return "";
    }
}
