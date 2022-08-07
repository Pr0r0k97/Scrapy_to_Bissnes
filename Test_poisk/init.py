import Remont_office
import Blago_terr
import Burrovie_rabot
import Otdelka_office
import Otdelka_pom
import Prokladka_kabel
import Prom_stroitelstvo
import Remont_kartir
import Stroitel_domov
import Stroitel_kompany
import Svar_rabot
import Vasad_remont
import Visot_rabot



def main():
    rem = Remont_office.main()
    stroiteli = Stroitel_kompany.main()
    blago = Blago_terr.main()
    burr = Burrovie_rabot.main()
    otdel_of = Otdelka_office.main()
    otdel_pom = Otdelka_pom.main()
    pklad_kab = Prokladka_kabel.main()
    Prom_stro = Prom_stroitelstvo.main()
    rem_kv = Remont_kartir.main()
    Stroi_dom = Stroitel_domov.main()
    stroiteli = Stroitel_kompany.main()
    svar_rab = Svar_rabot.main()
    vasad_re = Vasad_remont.main()
    visot_rab = Visot_rabot.main()



if __name__ == '__main__':
        main()