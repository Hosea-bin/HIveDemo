package com.atguigu.etl;


/**
 * Etl工具类
 */
public class EtlUtils {

    /**
     * 清洗 video的数据
     * n1cEq1C8oqQ	Pipistrello	525	Comedy	125	1687	4.01	363	141
     * eprHhmurMHg	i30NkTJOrak	2XtLgZol5wI	3nH5Tccz8EQ	bSPVayE0NhE	sEqCkwPmQ_w	hut3VRL5XRE	bWlPSLUT-6U	dsBTo5LExr0	7PSvpPXppXA	yLup8wjbSIo	lbf4d1pZI9c	uRQYan_-CTQ	gnpvEvuiFoQ	F2_5KOnSsfI	DINu35v3eMU	9uSiyn7t_0o	YfShxdbAJS8	ssdfqTwZXY0	z5wDjq8o60c
     * 清洗规则：
     * 1、判断传入的数据是否完整
     * 2、将视频类别的空格去掉
     * 3、相关视频通过&连接
     */
    public static String etlData(String srcData){
        StringBuffer sbs =new StringBuffer();
        //通过\t切割
        String[] datas = srcData.split("\t");
        if(datas.length<9){
            return null;
        }
        //2、处理视频类别的空格
        if(datas.length>=4) {
            datas[3] = datas[3].replaceAll(" ", "");
        }
        //3、处理相关视频
        for(int i=0;i<datas.length;i++){
            if(i<9){
                //没有相关视频
                if(i<datas.length-1){
                    sbs.append(datas[i]).append("\t");
                }else {
                    sbs.append(datas[i]);
                }
            }else {
                //有相关视频
                if(i<datas.length-1){
                    sbs.append(datas[i]).append("&");
                }else {
                    sbs.append(datas[i]);
                }
            }
        }
        return sbs.toString();
    }

    public static void main(String[] args) {
        String result=etlData("n1cEq1C8oqQ\tPipistrello\t525\tComedy\t125\t1687\t4.01\t363\t141\teprHhmurMHg\ti30NkTJOrak\t2XtLgZol5wI\t3n");
        System.out.println(result);
    }
}
