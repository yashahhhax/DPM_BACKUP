SELECT

    ingredients.number as FLPM_SKU,

    LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(RTRIM(max(CONVERT(nvarchar(100), ingredients.id))), CHAR(10), ''), CHAR(13), ''), CHAR(9), ''), CHAR(160), '')))

     as Item_Description,

     LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(max(Replace(Replace(convert(varchar,ingredients.barcode),CHAR(9),''),CHAR(13)+CHAR(10),'')), CHAR(10), ''), CHAR(13), ''), CHAR(9), ''), CHAR(160), '')))

     as Barcode,

max(zetup62.id) as Type,

    max(sup.Supplier_Name) as Supplier,

    max(zetup64.id) as Gender,

    max(zetup61.id) as Line,

    max(zetup51.id) as Sub_category,

    max(zetup63.id) as Theme_Family,

    max(zetup65.id) as Size,

    LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(max(zetup66.id), CHAR(10), ''), CHAR(13), ''), CHAR(9), ''), CHAR(160), ''))) as Style_Number,

    LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(max(Replace(Replace(CONVERT(varchar,ingredients.description),CHAR(9),''),CHAR(13)+CHAR(10),'')), CHAR(10), ''), CHAR(13), ''), CHAR(9), ''), CHAR(160), '')))

     as Vendor_SKU,

       max(farah16.id) as SWAD_Character,

    max(farah03.id) as UDA_Character, 

    max(farah05.id) as UDA_Type,

       max(farah10.id) as Line_Ride,

    max(ingredients.enteredon) as enteredon

from ingredients WITH (NOLOCK)

inner join zetup21 WITH (NOLOCK) on ingredients.dbtype = zetup21.intnum

inner join zetup05 WITH (NOLOCK) on ingredients.dbclass = zetup05.intnum

inner join zetup51 WITH (NOLOCK) on ingredients.subtype2 = zetup51.intnum

inner join zetup61 WITH (NOLOCK) on ingredients.retail1 = zetup61.intnum

inner join zetup62 WITH (NOLOCK) on ingredients.retail2 = zetup62.intnum

inner join zetup63 WITH (NOLOCK) on ingredients.retail3 = zetup63.intnum

inner join zetup64 WITH (NOLOCK) on ingredients.retail4 = zetup64.intnum

inner join zetup65 WITH (NOLOCK) on ingredients.retail5 = zetup65.intnum

inner join zetup66 WITH (NOLOCK) on ingredients.retail6 = zetup66.intnum

inner join products WITH (NOLOCK) on ingredients.intnum=products.linkitem

inner join zetup13 WITH (NOLOCK) on ingredients.unit1 = zetup13.intnum

inner join ingother WITH (NOLOCK) on ingother.intnum = ingredients.intnum

left outer join suppliers WITH (NOLOCK) on ingother.company1 = suppliers.intnum

left outer join farah04 WITH (NOLOCK) on ingother.ingother2 = farah04.intnum

left outer join farah03 WITH (NOLOCK) on ingother.ingother4 = convert(nvarchar,farah03.intnum)

left outer join farah05 WITH (NOLOCK) on ingother.ingother6 = Farah05.intnum

left outer join farah06 WITH (NOLOCK) on ingother.ingother5 = Farah06.intnum

left outer join farah09 WITH (NOLOCK) on ingother.ingother12 = Farah09.intnum

left outer join farah10 WITH (NOLOCK) on ingother.ingother14 = Farah10.intnum

left outer join farah16 WITH (NOLOCK) on ingother.ingother15 = Farah16.intnum

left outer join farah08 WITH (NOLOCK) on ingother.ingother16 = Farah08.intnum

Outer Apply

    (

       Select top 1 b.intnum, b.id as Supplier_Name from rqadetail a  WITH (NOLOCK)

        left join suppliers b  WITH (NOLOCK) on a.company=b.intnum

        where item= ingredients.intnum

        Order by a.postdate desc

    ) sup
Where ingredients.id Not Like 'zz%' and ingredients.id!='IT Test'
group by ingredients.number
