package com.evayInfo.Inglory.Test;

import java.util.Hashtable;

public class AutoLabel {

    private static Hashtable<String, String[]> governHash = new Hashtable();
    private static Hashtable<String, String[]> secondHash = new Hashtable();

    /**
     * @param title 文章标题
     *              content 文章内容
     *              label 返回文章的标签
     */
    public String autoLabel(String title, String content) {
        //三四级标签清单
        String[] engineering = {"工程建筑", "房屋建筑", "防水工程", "给排水", "钢结构", "基础工程", "市政工程",
                "桩基", "土石方", "土地整理", "建筑工程", "电梯", "中央空调", "冷却塔", "热水器", "卫生洁具"};
        String[] transportation = {"交通运输", "铁路货运", "轨道车辆", "铜材", "隧道灯具", "道路标志", "路灯",
                "护栏", "信号灯", "电子警察", "仓储物流", "货架", "托盘", "叉车", "升降机", "冷库", "搬运工具"};
        String[] environmental = {"环保绿化", "广场工程", "园林", "景观", "绿化", "亮光", "废弃物加工", "垃圾处理",
                "环境工程", "公厕", "垃圾桶", "喷泉", "广告牌", "环卫设施", "环卫车辆", "环卫作业", "焚烧炉", "环卫设备"};
        String[] healthCare = {"医疗设备", "监测仪", "病床", "分析仪", "多普勒", "中心供氧", "医用材料",
                "注射器", "注射液", "纱布", "无尘纸", "假肢", "急救箱", "医疗医药", "中药饮片", "疫苗", "消毒液", "医保药品"};
        String[] instrument = {"仪器仪表", "分析仪器", "色谱仪", "光谱仪", "光度计", "质谱仪", "分析仪器", "光学仪器",
                "显微镜", "水准仪", "经纬仪", "测距仪", "望远镜", "测量仪表", "试验机", "粒度仪", "激光检测", "探伤仪", "色彩分析"};
        String[] waterAndEle = {"水利水电", "水利工程", "给水", "给排水", "水库", "堤坝", "节水", "饮水器",
                "电力设备", "水电站", "热电", "配电", "风力发电", "水利控制", "闸门", "启闭机", "水泵"};
        String[] energyAndChe = {"能源化工", "矿业设备", "采煤机", "综测仪", "掘进机", "破碎机", "提升机",
                "运输机械", "转轴", "磁选机", "皮带机", "钻机", "化工设备", "化工泵", "阀门", "压力变送", "储罐"};
        String[] secuAndPro = {"弱电安防", "安防", "弱电", "系统集成", "楼宇对讲", "电子巡更", "消防设备",
                "警用装备", "防弹服", "防暴服", "头盔", "强光手电", "对讲机"};
        String[] education = {"办公文教", "教育产品", "电子白板", "多媒体", "图书", "课桌椅", "体育器材", "篮球架",
                "乒乓球桌", "羽毛球", "健身器材", "办公家具", "会议桌", "办公桌", "沙发", "电脑桌", "办公椅"};
        String[] communication = {"通讯电子", "通信工程", "接收机", "交换机", "路由器", "机顶盒",
                "通信电缆", "通信设备", "通信产品", "信息化建设", "应急指示", "卫星通信", "光纤通信", "网络办公", "电信设备"};
        String[] mechanical = {"机械设备", "工程机械", "混凝土搅拌", "搅拌站", "压路机", "吊塔", "起重机", "机械加工",
                "机车电台", "钻床", "镗床", "磨床", "加工中心", "混合设备", "泵", "阀门", "风机", "压缩机", "空气泵", "空气处理"};
        String[] agriculture = {"农林牧渔", "农业机械", "播种机", "割草机", "割灌机", "收割机", "农机机电",
                "畜牧设备", "耳标", "动物标本", "挤奶机", "繁育", "农牧机械", "农用化工", "化肥", "尿素", "追肥", "复合肥", "肥料", "除草剂"};

        //第二级清单
        String[] titleHashKey = {"政府", "学校"};
        String institution = "事业单位";
        String[] HashKey = {"企业", "社会团体"};
        String[] governValue = {"政府", "局", "法院", "检察院", "办公厅", "总局"};
        String[] enterpValue = {"企业", "集团", "股份"};
        String[] schoolValue = {"学校", "大学", "学院", "教育", "医学院", "幼儿园", "小学"};
        String[] socialValue = {"联合会", "基金会", "协会", "促进会", "工会"};

        governHash.put(titleHashKey[0], governValue);
        governHash.put(titleHashKey[1], schoolValue);
        secondHash.put(HashKey[0], enterpValue);
        secondHash.put(HashKey[1], socialValue);

        //第一级标签
        String[] firstLabel = {"竞争性谈判", "竞争性磋商", "招标", "采购", "需求", "邀标", "单一来源"};

        //标签初始化为空
        String label = null;

        //第一级标签 唯一的从标题中获取第一级标签
        for (String firstItem : firstLabel) {
            if (title.contains(firstItem)) {
                label = firstItem + "公告";
                break;
            }
        }

	    /*第二级标签  第二级标签是唯一的包括政府、学校、企业、事业单位、社会团体
              首先通过标题判断是否是政府和学校*/
        boolean secondIsOk = false;
        for (String secondLabel : titleHashKey) {
            if (secondIsOk) break;
            for (String secondItem : governHash.get(secondLabel)) {
                if (title.contains(secondItem)) {
                    if (label == null) label = secondLabel;
                    else label = label + ";" + secondLabel;
                    secondIsOk = true;
                    break;
                }
            }
        }

        //如果前面没有在通过能容对事业单位和政府进行判断
        boolean contentIsOk = false;
        if (!secondIsOk) {
            for (String secondLabel : HashKey) {
                if (contentIsOk) break;
                for (String secondItem : secondHash.get(secondLabel)) {
                    if (content.contains(secondItem)) {
                        if (label == null) label = secondLabel;
                        else label = label + ";" + secondLabel;
                        contentIsOk = true;
                        break;
                    }
                }
            }
        }

        //最后是事业单位
        if (!contentIsOk && !secondIsOk) {
            if (label == null) label = institution;
            else label = label + ";" + institution;
        }

        //第三四级标签 首先判断四级标签是否存在，如果存在加上相应的三级标签
        String engine = thirdAndFour(title, content, engineering);
        if (engine == null) {
            String transport = thirdAndFour(title, content, transportation);
            if (transport == null) {
                String environment = thirdAndFour(title, content, environmental);
                if (environment == null) {
                    String health = thirdAndFour(title, content, healthCare);
                    if (health == null) {
                        String instru = thirdAndFour(title, content, instrument);
                        if (instru == null) {
                            String water = thirdAndFour(title, content, waterAndEle);
                            if (water == null) {
                                String energy = thirdAndFour(title, content, energyAndChe);
                                if (energy == null) {
                                    String security = thirdAndFour(title, content, secuAndPro);
                                    if (security == null) {
                                        String educate = thirdAndFour(title, content, education);
                                        if (educate == null) {
                                            String communicate = thirdAndFour(title, content, communication);
                                            if (communicate == null) {
                                                String mechain = thirdAndFour(title, content, mechanical);
                                                if (mechain == null) {
                                                    String agriculte = thirdAndFour(title, content, agriculture);
                                                    if (agriculte != null) label = label + ";" + agriculte;
                                                } else label = label + ";" + mechain;
                                            } else label = label + ";" + communicate;
                                        } else label = label + ";" + educate;
                                    } else label = label + ";" + security;
                                } else label = label + ";" + energy;
                            } else label = label + ";" + water;
                        } else label = label + ";" + instru;
                    } else label = label + ";" + health;
                } else label = label + ";" + environment;
            } else label = label + ";" + transport;
        } else label = label + ";" + engine;
        //  System.out.println(label);
        return label;
    }

    /**
     * @param title 文章标题
     *              content 文章内容
     *              labelArray 标签清单
     *              label 返回的三四级标签
     *              第三四级标签自动产生方法
     */
    public String thirdAndFour(String title, String content, String[] labelArray) {
        String label = null;
        if (title != null && content != null) {
            for (int i = 1; i < labelArray.length; i++) {
                String tiContent = title + content;
                if (tiContent.contains(labelArray[i])) {
                    if (label == null) label = labelArray[i];
                    else label = label + ";" + labelArray[i];
                }
            }
            if (label != null) label = labelArray[0] + ";" + label;
        }
        return label;
    }

    public static void main(String[] args) {
        String label = new AutoLabel().autoLabel("济南市槐荫区养老服务从业人员培训项目需求公示", "");
        System.out.println(label);
    }
}
