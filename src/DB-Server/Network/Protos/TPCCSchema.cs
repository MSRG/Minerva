using System;
using MemoryPack;

namespace Minerva.DB_Server.Network.Protos;

[MemoryPackable]
public partial class Warehouse 
{

    public long W_ID { get; set; }

    public string W_NAME { get; set; }

    public string W_STREET_1 { get; set; }

    public string W_STREET_2 { get; set; }

    public string W_CITY { get; set; }

    public string W_STATE { get; set; }

    public string W_ZIP { get; set; }

    public double W_TAX { get; set; }

    public double W_YTD { get; set; }


}

[MemoryPackable]
public partial class District 
{


    public long D_ID { get; set; }

    public long D_W_ID { get; set; }

    public string D_NAME { get; set; }

    public string D_STREET_1 { get; set; }

    public string D_STREET_2 { get; set; }

    public string D_CITY { get; set; }

    public string D_STATE { get; set; }

    public string D_ZIP { get; set; }

    public double D_TAX { get; set; }

    public double D_YTD { get; set; }

    public long D_NEXT_O_ID { get; set; }


}

[MemoryPackable]
public partial class Customer
{

    public long C_ID { get; set; }

    public long C_D_ID { get; set; }

    public long C_W_ID { get; set; }

    public string C_FIRST { get; set; }

    public string C_MIDDLE { get; set; }

    public string C_LAST { get; set; }
    
    public string C_STREET_1 { get; set; }

    public string C_STREET_2 { get; set; }

    public string C_CITY { get; set; }

    public string C_STATE { get; set; }

    public string C_ZIP { get; set; }

    public string C_PHONE { get; set; }

    public long C_SINCE { get; set; }

    public string C_CREDIT { get; set; }

    public long C_CREDIT_LIM { get; set; }

    public double C_DISCOUNT { get; set; }

    public double C_BALANCE { get; set; }

    public double C_YTD_PAYMENT { get; set; }

    public long C_PAYMENT_CNT { get; set; }
    
    public int C_DELIVERY_CNT { get; set; }
    
    public string C_DATA { get; set; }


}

[MemoryPackable]
public partial class Item
{

    public long I_ID { get; set; }

    public long I_IM_ID { get; set; }

    public string I_NAME { get; set; }

    public double I_PRICE { get; set; }

    public string I_DATA { get; set; }



}

[MemoryPackable]
public partial class Stock 
{

    public long S_I_ID { get; set; }

    public long S_W_ID { get; set; }

    public long S_QUANTITY { get; set; }

    public string S_DIST_01 { get; set; }

    public string S_DIST_02 { get; set; }

    public string S_DIST_03 { get; set; }

    public string S_DIST_04 { get; set; }
    
    public string S_DIST_05 { get; set; }

    public string S_DIST_06 { get; set; }

    public string S_DIST_07 { get; set; }
    
    public string S_DIST_08 { get; set; }
    
    public string S_DIST_09 { get; set; }
    
    public string S_DIST_10 { get; set; }
    
    public long S_YTD { get; set; }
    
    public long S_ORDER_CNT { get; set; }
    
    public long S_REMOTE_CNT { get; set; }
    
    public string S_DATA { get; set; }

}

[MemoryPackable]
public partial class History
{
 


    public long H_C_ID { get; set; }

    public long H_C_D_ID { get; set; }

    public long H_C_W_ID { get; set; }

    public long H_D_ID { get; set; }

    public long H_W_ID { get; set; }
    
    public long H_DATE { get; set; }

    public double H_AMOUNT { get; set; }

    public string H_DATA { get; set; }


}

[MemoryPackable]
public partial class NewOrder 
{


    public long NO_O_ID { get; set; }
    
    public long NO_D_ID { get; set; }
    
    public long NO_W_ID { get; set; }

}

[MemoryPackable]
public partial class Order
{


    public long O_ID { get; set; }

    public long O_C_ID { get; set; }

    public long O_D_ID { get; set; }

    public long O_W_ID { get; set; }

    public long O_ENTRY_D { get; set; }

    public long O_CARRIER_ID { get; set; }

    public long O_OL_CNT { get; set; }

    public bool O_ALL_LOCAL { get; set; }


}

[MemoryPackable]
public partial class OrderLine
{




    public long OL_O_ID { get; set; }

    public long OL_D_ID { get; set; }

    public long OL_W_ID { get; set; }

    public long OL_NUMBER { get; set; }

    public long OL_I_ID { get; set; }

    public long OL_SUPPLY_W_ID { get; set; }

    public long OL_DELIVERY_D { get; set; }

    public long OL_QUANTITY { get; set; }

    public double OL_AMOUNT { get; set; }

    public string OL_DIST_INFO { get; set; }
}