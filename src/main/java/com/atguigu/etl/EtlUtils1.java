package com.atguigu.etl;

/**
 * ETL工具类
 */
public class EtlUtils1 {
    /**
     * 清洗 video的数据
     *
     * 清洗规则:
     * 1. 判断传入的数据是否完整
     * 2. 将视频类别中的  空格去掉  People & Blogs  ==> People&Blogs
     * 3. 将相关视频通过&拼接     udr9sLkoZ0s	3IU1GyX_zio ==>udr9sLkoZ0s&3IU1GyX_zio
     *
     *
     * 一条数据:
     * 有相关视频:
     * fQShwYqGqsw	lonelygirl15	736	People & Blogs	133	151763	3.01	666	765	fQShwYqGqsw	LfAaY1p_2Is	5LELNIVyMqo	vW6ZpqXjCE4	vPUAf43vc-Q	ZllfQZCc2_M	it2d7LaU_TA	KGRx8TgZEeU	aQWdqI1vd6o	kzwa8NBlUeo	X3ctuFCCF5k	Ble9N2kDiGc	R24FONE2CDs	IAY5q60CmYY	mUd0hcEnHiU	6OUcp6UJ2bA	dv0Y_uoHrLc	8YoxhsUMlgA	h59nXANN-oo	113yn3sv0eo
     * 没有相关视频:
     * SDNkMu8ZT68	w00dy911	630	People & Blogs	186	10181	3.49	494	257
     */

    public static String  etlData(String srcData){
        StringBuilder sbs = new StringBuilder();

        //通过\t切割数据
        String[] datas = srcData.split("\t");
        //1.判断数据是否完整
        if(datas.length < 9){
            return null ;
        }
        //2. 处理视频类别的空格
        datas[3] = datas[3].replaceAll(" ","");

        //3. 处理相关视频
        for (int i = 0; i < datas.length; i++) {
            if(i < 9 ){
                //思考没有相关视频的情况
                if(i< datas.length-1){
                    sbs.append(datas[i]).append("\t");
                }else{
                    sbs.append(datas[i]);
                }
            }else{
                //思考有相关视频的情况
                if(i<datas.length -1){
                    sbs.append(datas[i]).append("&");
                }else{
                    sbs.append(datas[i]);
                }
            }
        }
        return sbs.toString() ;
    }

    public static void main(String[] args) {

        String result =
               etlData("fQShwYqGqsw\tlonelygirl15\t736\tPeople & Blogs\t133\t151763\t3.01\t666");
        System.out.println(result);
    }

}
