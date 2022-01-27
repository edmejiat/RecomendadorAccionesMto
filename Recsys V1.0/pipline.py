import re
import pandas as pd
import math
import base64

class Processing:
    
    def makebd(self, df):
        '''Esta función realiza los primeros filtros para tener un df para empezar a trabajar
           Argumentos: df -> base de datos de SAP
           Return: recomend1 -> recomend1 normalizada
        '''
        df.loc[df['Texto Ampliado'].isnull(), 'Texto Ampliado' ] = 'Sin info adicional'
        df.loc[df['Texto'].isnull(), 'Texto' ] = 'Sin aviso pendiente'
        df['Valor medido'] = df['Valor medido'].apply(lambda x: str(x).replace('99', '9'))
        df['Valor medido'] = df['Valor medido'].apply(lambda x: float(str(x).replace(',', '.')))
        df['Valor medido'] = df['Valor medido'].apply(lambda x: math.floor(x))
        c = ['Equipo', 'Posición medida', 'Valor medido', 'Fecha', 'Texto Ampliado', 'Texto']
        recomend = df[c]
        recomend.Texto = recomend.Texto.str.replace('/ /', '//')
        recomend['Texto Ampliado'] = recomend['Texto Ampliado'].str.replace('/ /', '//')
        recomend1 = recomend[(recomend['Valor medido'] == 1) | (recomend['Valor medido'] == 2)]
        recomend1 = recomend1[recomend1['Texto'] != 'Sin aviso pendiente']
        recomend1['texto_final']= recomend1.apply(lambda x: x['Texto Ampliado'] if (x['Texto']!=x['Texto Ampliado']) and (x['Texto Ampliado']!='Sin info adicional') else x['Texto'], axis = 1)
        recomend1 = recomend1.drop(columns = ['Texto Ampliado', 'Texto'])
        return recomend1

    def standardized_texts(self, df, lis):
        '''Esta función normaliza las columnas de texto y saca agrega una columna de perfil catalogo
           Argumentos: df -> df recomend1, 
                       lis -> lista que contiene los perfil catalogo para cada equipo
           Return: recomend1 -> primera base de datos para normalizar 
        '''
        def dep_texto(y):
            x = re.sub('.*\. ', '', y)
            z = re.sub('.*\: ', '', x)
            return z
            
        def reemplazar(x):
            x = x.replace('PRESENCIA DE HUMEDAD.', '')
            x = x.replace('DESVIACIÓN ÚLTIMO RESULTADO RESPECTO A LA MEDIA.', '')
            x = x.replace('Héctor Fabio Mejía Restrepo Dirección Mantenimiento', '')
            x = x.replace('CONDICIÓN CUBA.', '')
            x = x.replace('3-HUMEDAD EN PAPEL:', '')
            x = x.replace('BUJE', 'BUJ')
            x = x.replace('3-OIL.TAP=DIAG/3-BUJ', '3-OIL.TAP=DIAG//3-BUJ')
            x = x.replace('3-OIL.TAP=DIAG/2-BUJ', '3-OIL.TAP=DIAG//2-BUJ')
            x = x.replace('2-OIL.TAP=DIAG/3-BUJ', '2-OIL.TAP=DIAG//3-BUJ')
            x = x.replace('2-OIL.TAP=DIAG/2-BUJ', '2-OIL.TAP=DIAG//2-BUJ')
            x = x.replace('6-HUM/2-OIL.CORR', '6-HUM//2-OIL.CORR')
            x = x.replace('6-HUM/9-OIL.CORR', '6-HUM//9-OIL.CORR')
            x = x.replace('6-HUM/8-HUM_PAP', '6-HUM//8-HUM_PAP')
            x = x.replace('6-HUM/6-HUM_PAP', '6-HUM//6-HUM_PAP')
            x = x.replace('1-DGA=T-ACE(C)/DP/(D)/9-OIL=9-FQ/9-DIE', '1-DGA=T-ACE(C)/DP/(D)//9-OIL=9-FQ/9-DIE')
            return x
            
        def per_cat(x, pc):
            for j in range(len(pc)):
                if x == int(pc[j][0]):
                    pp = pc[j][1]
                    break
                else:
                    pp = ''
            return pp
            
        recomend1 = df.copy()
        recomend1['texto_fallas'] = recomend1.texto_final.apply(lambda x: dep_texto(x))
        recomend1 = recomend1.drop(columns=['texto_final'])
        recomend1.texto_fallas = recomend1.texto_fallas.apply(lambda x: reemplazar(x))
        recomend1.texto_fallas = recomend1.texto_fallas.str.replace(' /', '//')
        recomend1.texto_fallas = recomend1.texto_fallas.str.replace(' ', '')
        recomend1 = recomend1[recomend1.texto_fallas.str.contains('/')]
        recomend1 = recomend1[recomend1.texto_fallas.str.contains(r'[A-Z]+', regex = True)]
        recomend1.texto_fallas = recomend1.texto_fallas.apply(lambda x: reemplazar(x))
        pc = lis
        recomend1['perfil_catalogo'] = recomend1['Equipo'].apply(lambda x: per_cat(x, pc))
        recomend1 = recomend1.drop_duplicates(subset=['Equipo', 'Valor medido', 'Fecha'])
        recomend1['id_unique'] = recomend1.apply(lambda x: str(x['Equipo']) + str(x['Valor medido']) + str(x['Fecha']), axis = 1)
        recomend1['id_unique'] = recomend1['id_unique'].str.replace('/', '')
        recomend1['id_unique'] = recomend1['id_unique'].str.replace('-', '')
        recomend1['id_unique'] = recomend1['id_unique'].apply(lambda x: base64.b64encode(x.encode()).decode())
        recomend1.reset_index(inplace=True)
        recomend1.drop(columns = ['index'], inplace= True)
        return recomend1
    
    def bd_principal(self, df):
        '''Esta función crea un df donde las columnas corresponden al texto para cada tipo de falla principal
           Argumentos: df -> recomend1
           Return: f_principales -> df con las columnas diferencias por falla principal
        '''
        def oh(x):
            if 'HUM' in x:
                if 'PAP' in x:
                    pass
                else:
                    x = x.replace('HUM', 'HUMP')
            elif 'OIL' in x:
                if 'TAP' in x:
                    pass
                elif 'CORR' in x:
                    pass
                else:
                    x = x.replace('OIL', 'OILP')
            else:
                pass
            return x

        recomend1 = df.copy()
        text = recomend1.texto_fallas.str.split('//', expand = True)
        text = text.replace([None], '')
        for i in range(len(text.columns)-1):
            text[i] = text[i].apply(lambda x: oh(x))
        principal = ['DGA', 'OILP', 'OIL.TAP', 'OIL.CORR', 'BUJ', 'INSP', 'HUMP', 'HUM_PAP']
        f_principales = pd.DataFrame(columns = principal)
        for h in range(len(text)):
            d = []
            t = ''
            for i in range(len(principal)):
                for j in range(len(text.columns)):
                    if principal[i] in text[j].iloc[h]:
                        t = text[j].iloc[h]
                        if ('INSP' in t) and ('BUJ' in t) and (principal[i] == 'BUJ'):
                            t = ''
                        else:
                            t = t
                            break
                    else:
                        t = ''
                d.append(t)
            f_principales = f_principales.append({principal[0]:d[0], principal[1]:d[1], principal[2]:d[2] , principal[3]:d[3], principal[4]:d[4], principal[5]:d[5], principal[6]:d[6], principal[7]:d[7]}, ignore_index=True)
        return f_principales

    def standardized_columns(self, df):
        '''Esta función normaliza los textos por falla principal
           Argumentos: df -> df f_principales con las columnas diferenciadas por falla principal
           Return: f_principales -> df de entrada con las columnas normalizadas 
        '''
        def parent(x):
            x = re.sub(r"\([^()]*\)", "", x)
            x = x.strip('/')
            return x

        def d(x):
            if 'DELTA' in x:
                if ('DELTA-A' in x) or ('DELTA-B' in x) or ('DELTA-C' in x):
                    pass
                else:
                    x = x.replace('DELTA', 'DELTA-A')
            else:
                pass
            return x

        def cub(x):
            if 'CUB' in x:
                m = re.findall(r'[0-9]-CUB', x)
                if len(m) == 2:
                    if float(m[0][0]) < float(m[1][0]):
                        x = x.replace(m[1], '')
                        x = x.replace('//', '/')

                    elif float(m[0][0]) == float(m[1][0]):
                        x = x.replace(m[0], '')
                        x = x + m[0]
                        x = x.replace('//', '/')
                    else:
                        x = x.replace(m[0], '')
                        x = x.replace('//', '/')
                else:
                    pass
            else:
                pass
            
            return x

        f_principales = df.copy()
        for col in f_principales.columns:
            f_principales[col] = f_principales[col].apply(lambda x: str(x).strip('/'))
        f_principales.DGA = f_principales.DGA.apply(lambda x: parent(str(x)))
        f_principales.DGA = f_principales.DGA.str.replace('PD', 'DP')
        f_principales.DGA = f_principales.DGA.str.replace('T-PAP', 'T.PAP')
        f_principales.DGA = f_principales.DGA.str.replace('T-ACE', 'T.ACE')
        f_principales.OILP = f_principales.OILP.str.replace('DIEL', 'DIE')
        f_principales.OILP = f_principales.OILP.str.replace('DIE.PF', 'DIE')
        f_principales.OILP = f_principales.OILP.str.replace('DIE.', 'DIE')
        f_principales.OILP = f_principales.OILP.str.replace('8-SILI.GEL/8-ING.ANIM/8-RES.CALEF', '')
        f_principales['OIL.TAP'] = f_principales['OIL.TAP'].apply(lambda x: str(x).replace('DI', 'DIAG') if x == '3-OIL.TAP=DI' else x)
        f_principales['OIL.TAP'] = f_principales['OIL.TAP'].apply(lambda x: d(x))
        f_principales.BUJ = f_principales.BUJ.str.replace('/9-SILI.GEL/9-FUG.ACEI/9-FUN.TERM/9-NIV.ACEI/9-SIST.ENF', '')
        f_principales.INSP = f_principales.INSP.str.replace('P.CUB', 'CUB')
        f_principales.INSP = f_principales.INSP.apply(lambda x: cub(x))
        f_principales.INSP = f_principales.INSP.apply(lambda x: str(x).strip('/'))
        return f_principales

    def creation_bds(self, pps, r1):
        '''Esta función crea dfs para cada falla principal que no este vacía
           Argumentos: pps -> df f_principales normalizada, 
                       r1 -> df recomend1 normalizada
           Return:  kbds -> lista con losdfs de las fallas principales no vacías, 
                    kn_bds -> lista con los nombres de las fallas principales que no estaban vacías
        '''
        f_principales = pps.copy()
        recomend1 = r1.copy()

        ##DGA
        dga = f_principales.DGA.str.split('=', expand = True)
        dga2 = dga[1].str.split('/', expand = True)
        dga = dga.replace([None], '')
        dga2 = dga2.replace([None], '')
        dgac = ['ARC', 'T.ACE', 'T.PAP', 'DP']
        dga3 = pd.DataFrame(columns = dgac)
        for h in range(len(dga2)):
            d = []
            t = ''
            for i in range(len(dgac)):
                for j in range(len(dga2.columns)):
                    if dgac[i] in dga2[j].iloc[h]:
                        t = dga2[j].iloc[h]
                        break
                    else:
                        t = ''
                d.append(t)
            dga3 = dga3.append({dgac[0]:d[0], dgac[1]:d[1], dgac[2]:d[2] , dgac[3]:d[3]}, ignore_index=True)
        dga = dga.drop(columns = [1])
        dga.columns = ['DGA']
        dga = pd.concat([dga, dga3], axis = 1)
        dga.DGA = dga.DGA.str.replace('-DGA', '')
        dga['perfil_catalogo'] = recomend1.perfil_catalogo
        dga['id_unique'] = recomend1.id_unique
        dga.DGA = dga.DGA.apply(lambda x: int(x) if x != '' else x)

        ##OILP
        oilp = f_principales.OILP.str.split('=', expand = True)
        oilp2 = oilp[1].str.split('/', expand = True)
        oilp = oilp.replace([None], '')
        oilp2 = oilp2.replace([None], '')
        oilpc = ['DIE', 'FQ']
        oilp3 = pd.DataFrame(columns = oilpc)
        for h in range(len(oilp2)):
            d = []
            t = ''
            for i in range(len(oilpc)):
                for j in range(len(oilp2.columns)):
                    if oilpc[i] in oilp2[j].iloc[h]:
                        t = oilp2[j].iloc[h]
                        break
                    else:
                        t = ''
                d.append(t)
            oilp3 = oilp3.append({oilpc[0]:d[0], oilpc[1]:d[1]}, ignore_index=True)
        oilp = oilp.drop(columns = [1])
        oilp.columns = ['OILP']
        oilp = pd.concat([oilp, oilp3], axis = 1)
        oilp.OILP = oilp.OILP.str.replace('-OILP', '')
        oilp['perfil_catalogo'] = recomend1.perfil_catalogo
        oilp['id_unique'] = recomend1.id_unique
        oilp.OILP = oilp.OILP.apply(lambda x: int(x) if x != '' else x)

        ##OIL.TAP
        oil_tap = f_principales['OIL.TAP'].str.split('=', expand = True)
        oil_tap2 = oil_tap[1].str.split('/', expand = True)
        oil_tap = oil_tap.replace([None], '')
        oil_tap2 = oil_tap2.replace([None], '')
        oil_tapc = ['DIAG', 'DELTA-A', 'DELTA-B', 'DELTA-C']
        oil_tap3 = pd.DataFrame(columns = oil_tapc)
        for h in range(len(oil_tap2)):
            d = []
            t = ''
            for i in range(len(oil_tapc)):
                for j in range(len(oil_tap2.columns)):
                    if oil_tapc[i] in oil_tap2[j].iloc[h]:
                        t = oil_tap2[j].iloc[h]
                        break
                    else:
                        t = ''
                d.append(t)
            oil_tap3 = oil_tap3.append({oil_tapc[0]:d[0], oil_tapc[1]:d[1], oil_tapc[2]:d[2], oil_tapc[3]:d[3]}, ignore_index=True)
        oil_tap = oil_tap.drop(columns = [1])
        oil_tap.columns = ['OIL.TAP']
        oil_tap = pd.concat([oil_tap, oil_tap3], axis = 1)
        oil_tap['OIL.TAP'] = oil_tap['OIL.TAP'].str.replace('-OIL.TAP', '')
        oil_tap['perfil_catalogo'] = recomend1.perfil_catalogo
        oil_tap['id_unique'] = recomend1.id_unique
        oil_tap['OIL.TAP']  = oil_tap['OIL.TAP'].apply(lambda x: int(x) if x != '' else x)

        ##OIL.CORR
        oil_corr = f_principales['OIL.CORR'].str.split('=', expand = True)
        oil_corr.columns = ['OIL.CORR']
        oil_corr['OIL.CORR'] = oil_corr['OIL.CORR'].str.replace('-OIL.CORR', '')
        oil_corr['perfil_catalogo'] = recomend1.perfil_catalogo
        oil_corr['id_unique'] = recomend1.id_unique
        oil_corr = oil_corr.replace([None], '')
        oil_corr['OIL.CORR'] = oil_corr['OIL.CORR'].apply(lambda x: int(x) if x != '' else x)

        ##BUJ
        buj = f_principales.BUJ.str.split('=', expand = True)
        buj.columns = ['BUJ']
        buj.BUJ = buj.BUJ.str.replace('-BUJ', '')
        buj['perfil_catalogo'] = recomend1.perfil_catalogo
        buj['id_unique'] = recomend1.id_unique
        buj = buj.replace([None], '')
        buj.BUJ = buj.BUJ.apply(lambda x: int(x) if x != '' else x)

        ##INSP
        insp = f_principales.INSP.str.split('=', expand = True)
        insp2 = insp[1].str.split('/', expand = True)
        insp = insp.replace([None], '')
        insp2 = insp2.replace([None], '')
        inspc = ['BUJ', 'ENF', 'M.OLTC', 'PRO-M', 'CUB', 'GAB', 'FUG.ACEI', 'COMP.ELEC', 'COMP.MEC', 'RES.CALEF']
        insp3 = pd.DataFrame(columns = inspc)
        for h in range(len(insp2)):
            d = []
            t = ''
            for i in range(len(inspc)):
                for j in range(len(insp2.columns)):
                    if inspc[i] in insp2[j].iloc[h]:
                        t = insp2[j].iloc[h]
                        break
                    else:
                        t = ''
                d.append(t)
            insp3 = insp3.append({inspc[0]:d[0], inspc[1]:d[1], inspc[2]:d[2] , inspc[3]:d[3], inspc[4]:d[4], inspc[5]:d[5], inspc[6]:d[6], inspc[7]:d[7], inspc[8]:d[8], inspc[9]:d[9]}, ignore_index=True)
        insp = insp.drop(columns = [1])
        insp.columns = ['INSP']
        insp = pd.concat([insp, insp3], axis = 1)
        insp.INSP = insp.INSP.str.replace('-INSP', '')
        insp['perfil_catalogo'] = recomend1.perfil_catalogo
        insp['id_unique'] = recomend1.id_unique
        insp = insp.replace([None], '')
        insp.INSP = insp.INSP.apply(lambda x: int(x) if x != '' else x)

        ##HUMP
        hump = f_principales.HUMP.str.split('=', expand = True)
        hump2 = hump[1].str.split('/', expand = True)
        hump = hump.replace([None], '')
        hump2 = hump2.replace([None], '')
        humpc = ['DIAG', 'DELTA', 'FECHA', 'SILI.GEL', 'T_M', 'FUG.ACEI', 'ING.ANIM', 'FUN.TERM', 'RES.CALEF', 'NIV.ACEI', 'SIST.ENF']
        hump3 = pd.DataFrame(columns = humpc)
        for h in range(len(hump2)):
            d = []
            t = ''
            for i in range(len(humpc)):
                for j in range(len(hump2.columns)):
                    if humpc[i] in hump2[j].iloc[h]:
                        t = hump2[j].iloc[h]
                        break
                    else:
                        t = ''
                d.append(t)
            hump3 = hump3.append({humpc[0]:d[0], humpc[1]:d[1], humpc[2]:d[2] , humpc[3]:d[3], humpc[4]:d[4], humpc[5]:d[5], humpc[6]:d[6], humpc[7]:d[7], humpc[8]:d[8], humpc[9]:d[9], humpc[10]:d[10]}, ignore_index=True)
        hump = hump.drop(columns = [1])
        hump.columns = ['HUMP']
        hump = pd.concat([hump, hump3], axis = 1)
        hump.HUMP = hump.HUMP.str.replace('-HUMP', '')
        hump['perfil_catalogo'] = recomend1.perfil_catalogo
        hump['id_unique'] = recomend1.id_unique
        hump = hump.replace([None], '')
        hump.HUMP = hump.HUMP.apply(lambda x: int(x) if x != '' else x)

        ##HUM.PAP
        hum_pap = f_principales['HUM_PAP'].str.split('=', expand = True)
        hum_pap2 = hum_pap[1].str.split('/', expand = True)
        hum_pap = hum_pap.replace([None], '')
        hum_pap2 = hum_pap2.replace([None], '')
        hum_papc = ['NO.EQ', 'DELTA', 'FECHA', 'DIAG', 'WHRT']
        hum_pap3 = pd.DataFrame(columns = hum_papc)
        for h in range(len(hum_pap2)):
            d = []
            t = ''
            for i in range(len(hum_papc)):
                for j in range(len(hum_pap2.columns)):
                    if hum_papc[i] in hum_pap2[j].iloc[h]:
                        t = hum_pap2[j].iloc[h]
                        break
                    else:
                        t = ''
                d.append(t)
            hum_pap3 = hum_pap3.append({hum_papc[0]:d[0], hum_papc[1]:d[1], hum_papc[2]:d[2], hum_papc[3]:d[3], hum_papc[4]:d[4]}, ignore_index=True)
        hum_pap = hum_pap.drop(columns = [1])
        hum_pap.columns = ['HUM_PAP']
        hum_pap = pd.concat([hum_pap, hum_pap3], axis = 1)
        hum_pap['HUM_PAP'] = hum_pap['HUM_PAP'].str.replace('-HUM_PAP', '')
        hum_pap['perfil_catalogo'] = recomend1.perfil_catalogo
        hum_pap['id_unique'] = recomend1.id_unique
        hum_pap = hum_pap.replace([None], '')
        hum_pap['HUM_PAP'] = hum_pap['HUM_PAP'].apply(lambda x: int(x) if x != '' else x)

        bds = [dga, oilp, oil_tap, oil_corr, buj, insp, hump, hum_pap]
        n_bds = ['dga', 'oilp', 'oil_tap', 'oil_corr', 'buj', 'insp', 'hump', 'hum_pap']
        kbds = []
        kn_bds = []
        for i in range(0,len(bds)):
            base = bds[i]
            if len(base[base[base.iloc[:,0].name] == '']) != len(base):
                kbds.append(base)
                kn_bds.append(n_bds[i])

        return kbds, kn_bds