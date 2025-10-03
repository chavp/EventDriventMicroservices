using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mti.OrderManagement.Persistence.Repositories;

namespace Mti.OrderManagement.Application.Parties.Repositories
{
    public class InMemoryPartyRepository : IPartyRepository
    {
        public IReadOnlyCollection<string> GetOrganizationTitles()
        {
            return new List<string>
            {
                "กรม",
                "กระทรวง",
                "กองทัพ",
                "กองทัพบก",
                "การรถไฟแห่งประเทศไทย",
                "เทศบาล",
                "ส่วนงานราชการ",
                "ส่วนราชการ",
                "ส่วนราชการกรมส่งเสริม",
                "สำนัก",
                "สำนักงาน",
                "สำนักงานชลประทาน",
                "สำนักงานตำรวจแห่งชาติ",
                "องค์การ",
                "องค์การบริ",
                "องค์การบริหารส่วนตำบล",
                "อบจ.",
                "มหาวิทยาลัย",
                "วิทยาลัย",
                "โรงเรียน",
                "โรงเเรียน",
                "คณะ",
                "บจ.",
                "บจก",
                "บจก.",
                "บมจ.",
                "บรรษัท",
                "บริษท",
                "บริษัท",
                "บริษัท คชเณศวร",
                "บริษัทมหาชนจำกัด",
                "บลจ.",
                "กลุ่ม",
                "กลุ่มบริษัท",
                "กิจการร่วมค้า",
                "หจก",
                "หจก.",
                "ห้าง",
                "ห้างหุ้นส่วน",
                "ห้างหุ้นส่วน จำกัด",
                "ห้างหุ้นส่วนจำกัด",
                "ห้างหุ้นส่วนสามัญ",
                "ห้างหุ้นส่วนสามัญนิติบุคคล",
                "ร้าน",
                "ธนาคาร",
                "กองทุนรวมอสังหาริมทรัย์",
                "วัด",
                "โบสถ์",
                "โครงการ",
                "คณะบุคคล",
                "ชุมชน",
                "นิติบุคคล",
                "มูลนิธิ",
                "สมาคม",
                "สหกรณ์",
                "สหกรณ์การเกษตร",
                "สำนักพิมพ์",
                "ศาล",
                "ศูนย์",
                "สถาน",
                "หน่วยงาน",
                "The Embassy",
                "NEW ZEALAND",
            };
        }
    }
}
