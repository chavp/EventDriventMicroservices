using Microsoft.EntityFrameworkCore;
using Mti.PartyManagement.Domain.Parties;
using Mti.PartyManagement.Domain.Parties.Types;

namespace Mti.PartyManagement.Persistence
{
    public class PartiesSeeder
    {
        protected readonly IDbContextFactory<PartiesContext> _dbFacto = null;
        public PartiesSeeder(IDbContextFactory<PartiesContext> dbFacto)
        {
            _dbFacto = dbFacto;
        }

        public void Seed()
        {
            using(var db = _dbFacto.CreateDbContext())
            {
                // seed org title
                // หน่วยงานรัฐ/ส่วนราชการ:
                savePartyTitle(db, "กรม", true);
                savePartyTitle(db, "กระทรวง", true);
                savePartyTitle(db, "กองทัพ", true);
                savePartyTitle(db, "กองทัพบก", true);
                savePartyTitle(db, "การรถไฟแห่งประเทศไทย", true);
                savePartyTitle(db, "เทศบาล", true);
                savePartyTitle(db, "ส่วนงานราชการ", true);
                savePartyTitle(db, "ส่วนราชการ", true);
                savePartyTitle(db, "ส่วนราชการกรมส่งเสริม", true);
                savePartyTitle(db, "สำนัก", true);
                savePartyTitle(db, "สำนักงาน", true);
                savePartyTitle(db, "สำนักงานชลประทาน", true);
                savePartyTitle(db, "สำนักงานตำรวจแห่งชาติ", true);
                savePartyTitle(db, "องค์การ", true);
                savePartyTitle(db, "องค์การบริ", true);
                savePartyTitle(db, "องค์การบริหารส่วนตำบล", true);
                savePartyTitle(db, "อบจ.", true);

                // สถาบันการศึกษา:
                savePartyTitle(db, "มหาวิทยาลัย", true);
                savePartyTitle(db, "วิทยาลัย", true);
                savePartyTitle(db, "โรงเรียน", true);
                savePartyTitle(db, "โรงเเรียน", true);
                savePartyTitle(db, "คณะ", true);

                // องค์กรทางธุรกิจ:
                savePartyTitle(db, "บจ.", true);
                savePartyTitle(db, "บจก", true);
                savePartyTitle(db, "บจก.", true);
                savePartyTitle(db, "บมจ.", true);
                savePartyTitle(db, "บรรษัท", true);
                savePartyTitle(db, "บริษท", true);
                savePartyTitle(db, "บริษัท", true);
                savePartyTitle(db, "บริษัท คชเณศวร", true);
                savePartyTitle(db, "บริษัทมหาชนจำกัด", true);
                savePartyTitle(db, "บลจ.", true);
                savePartyTitle(db, "กลุ่ม", true);
                savePartyTitle(db, "กลุ่มบริษัท", true);
                savePartyTitle(db, "กิจการร่วมค้า", true);
                savePartyTitle(db, "หจก", true);
                savePartyTitle(db, "หจก.", true);
                savePartyTitle(db, "ห้าง", true);
                savePartyTitle(db, "ห้างหุ้นส่วน", true);
                savePartyTitle(db, "ห้างหุ้นส่วน จำกัด", true);
                savePartyTitle(db, "ห้างหุ้นส่วนจำกัด", true);
                savePartyTitle(db, "ห้างหุ้นส่วนสามัญ", true);
                savePartyTitle(db, "ห้างหุ้นส่วนสามัญนิติบุคคล", true);
                savePartyTitle(db, "ร้าน", true);

                // สถาบันการเงิน
                savePartyTitle(db, "ธนาคาร", true);
                savePartyTitle(db, "กองทุนรวมอสังหาริมทรัย์", true);

                // องค์กรศาสนา:
                savePartyTitle(db, "วัด", true);
                savePartyTitle(db, "โบสถ์", true);

                // องค์กรอื่นๆ:
                savePartyTitle(db, "โครงการ", true);
                savePartyTitle(db, "คณะบุคคล", true);
                savePartyTitle(db, "ชุมชน", true);
                savePartyTitle(db, "นิติบุคคล", true);
                savePartyTitle(db, "มูลนิธิ", true);
                savePartyTitle(db, "สมาคม", true);
                savePartyTitle(db, "สหกรณ์", true);
                savePartyTitle(db, "สหกรณ์การเกษตร", true);
                savePartyTitle(db, "สำนักพิมพ์", true);
                savePartyTitle(db, "ศาล", true);
                savePartyTitle(db, "ศูนย์", true);
                savePartyTitle(db, "สถาน", true);
                savePartyTitle(db, "หน่วยงาน", true);
                savePartyTitle(db, "The Embassy", true);

                //องค์กรต่างประเทศ:
                savePartyTitle(db, "NEW ZEALAND", true);

                saveAssetRoleType(db, AssetRoleType.Owner, AssetRoleType.Owner);

                savePartyRoleType(db, PartyRoleType.Insured, PartyRoleType.Insured);
                savePartyRoleType(db, PartyRoleType.Invoice, PartyRoleType.Invoice);

                saveContactMechanismType(db, ContactMechanismType.Mobile, ContactMechanismType.Mobile);
                saveContactMechanismType(db, ContactMechanismType.HomePhone, ContactMechanismType.HomePhone);
                saveContactMechanismType(db, ContactMechanismType.OfficePhone, ContactMechanismType.OfficePhone);
                saveContactMechanismType(db, ContactMechanismType.Email, ContactMechanismType.Email);
                saveContactMechanismType(db, ContactMechanismType.MaimAddress, ContactMechanismType.MaimAddress);

                db.SaveChanges();
            }
        }

        private void savePartyTitle(PartiesContext db, string name, bool isOrganization)
        {
            var code = GenCode(name);
            if (!db.PartyTitles.Any(x => x.Code == code))
                db.Add(new PartyTitle(code) { Name = name, IsOrganization = isOrganization });
        }

        public static string? GenCode(string? data)
        {
            if (string.IsNullOrEmpty(data))
                return null;
            data = data.Trim();
            var codes = data.Split(" ", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

            return "#" + string.Join("_", codes).ToUpperInvariant();
        }

        private void saveAssetRoleType(PartiesContext db, string code, string name)
        {
            if (!db.AssetRoleTypes.Any(x => x.Code == code))
                db.Add(new AssetRoleType(code) { Name = name });
        }

        private void savePartyRoleType(PartiesContext db, string code, string name)
        {
            if (!db.PartyRoleTypes.Any(x => x.Code == code))
                db.Add(new PartyRoleType(code) { Name = name });
        }

        private void saveContactMechanismType(PartiesContext db, string code, string name)
        {
            if (!db.ContactMechanismTypes.Any(x => x.Code == code))
                db.Add(new ContactMechanismType(code) { Name = name });
        }
    }
}
