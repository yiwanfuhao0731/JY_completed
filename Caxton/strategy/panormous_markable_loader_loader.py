import datetime as dt
import math

import pandas as pd
import numpy as np
from scipy import interpolate as intrp

import panormus.data.open_data as od
import panormus.data.open_data_config as odc
from panormus.markable.loader.config.fixings_config import FIXINGS_OPENDATA_CONFIG as FODC
import panormus.markable.loader.loader_utils as blu
import panormus.quant.alib.utils as qau
import panormus.quant.vol_surface as qvs
import panormus.quant.fx_linear_config as fxlc

from panormus.data import cax
from panormus.config.settings import ALIB_CURVE_REGION
from panormus.utils.cache import (cache, cache_response, cache_put, clear_cache)


class BaseLoader:
    '''
    Base loader class
    '''

    def __init__(self):
        pass


class FixingsLoader(BaseLoader):
    '''
    Base class for interest rate fixings loader
    '''

    def __init__(self):
        super().__init__()


class FixingsLoaderOpenData(FixingsLoader):
    '''
    Loads interest rate fixings from open data
    '''

    cache_fixing_name = 'FixingsLoaderOpenData.get_fixing'
    cache_fixing_region = 'mem_10h'

    def __init__(self):
        super().__init__()

    @cache_response(cache_fixing_name, cache_fixing_region, True)
    def get_fixing(self, conv_name, date):
        ticker = FODC[conv_name]
        return od.df_for_observable_strings([ticker], start_date=date, end_date=date).values[0, 0]

    def get_fixings(self, conv_name, sdate, edate):
        ticker = FODC[conv_name]
        f = od.df_for_observable_strings([ticker], start_date=sdate, end_date=edate)
        f.columns = ['fix']
        f.index = pd.to_datetime(f.index).date
        return f

    def cache_fixing(self, fixing, conv_name, date):
        cache_put(self.cache_fixing_name, self.cache_fixing_region, fixing, conv_name, date)


class FxVolSurfaceLoader(BaseLoader):
    def __init__(self, holiday_oracle=None):
        super().__init__()
        self.holiday_oracle = qau.Holidays() if holiday_oracle is None else holiday_oracle

        self.COLS_TO_DROP = [
            'Date', 'FLY01', 'FLY10', 'FLY20', 'FLY30', 'FLY40', 'Live?', 'Pair', 'RR01', 'RR10', 'RR20', 'RR30',
            'RR40', 'Tenor']

        self.DELTA_NAMES = ['ATM', 'c01', 'c10', 'c20', 'c30', 'c40', 'p01', 'p10', 'p20', 'p30', 'p40']

    def get_fxvolsurface(self, pair, asof_date):
        def build_smile(row):
            strikes = [row[f'{delta_name} stk'] for delta_name in self.DELTA_NAMES]
            vols = [row[f'{delta_name} vol'] / 100.0 for delta_name in self.DELTA_NAMES]

            strikes, vols = (list(t) for t in zip(*sorted(zip(strikes, vols))))

            return qvs.StrikeInterpSmile(strikes, vols)

        holsname = f'{pair[:3].upper()}+{pair[3:].upper()}'
        hol_file = self.holiday_oracle[holsname]

        cax_fn = 'getvolsurface'
        cax_datatype = 'vols'
        cax_ticker = pair.upper() + ' FXV'
        cax_fields = None
        cax_filter = 'source=BAML'

        data_df = cax.cax_df(
            cax_fn, cax_datatype, [cax_ticker], [cax_fields], start_date=asof_date, end_date=asof_date,
            options=cax_filter)

        if 'previous_close' in data_df['Live?'].tolist():
            data_df = data_df[data_df['Live?'] == 'previous_close']
            min_date = min(data_df['Date'].tolist())
            data_df = data_df.set_index('Date').loc[min_date, :].reset_index().set_index('Maturity')
        else:
            data_df = data_df[data_df['Live?'] == 'latest_close'].set_index('Maturity')
        data_df.index = pd.to_datetime(data_df.index)
        data_df = data_df.drop(self.COLS_TO_DROP, axis=1)
        data_df['smiles'] = data_df.apply(build_smile, axis=1)

        smiles = data_df['smiles'].tolist()
        exp_ycfs = [qau.ac_bus_days_diff(asof_date, dte, hol_file) / 251. for dte in data_df.index]
       exp_ycfs, smiles = (list(t) for t in zip(*sorted(zip(exp_ycfs, smiles))))

        return qvs.FxVolSurface(asof_date, exp_ycfs, smiles)


class VolSurfaceLoader(BaseLoader):
    def __init__(self):
        super().__init__()

    def get_vol_surface(self, id, date):
        raise NotImplementedError

    def bulk_load_vol_surfaces(self, id, date_list):
        raise NotImplementedError


class EquityVolSurfaceLoader(VolSurfaceLoader):
    def __init__(self):
        super().__init__()
        self.fwd_money = [0.3 + 0.025 * i for i in range(80)]

    cache_vol_surf_name = 'EquityVolSurfaceLoader.get_vol_surface'
    cache_vol_surf_region_temp = 'mem_10h'
    cache_vol_surf_region_perm = 'disk'

    def get_cax_df(self, id, start_date, end_date, source='SOCGEN'):
        cax_fn = 'getvolsurface'
        cax_datatype = 'vols'
        cax_ticker = id.upper()
        cax_fields = None
        cax_filter = 'source=' + source

        data_df = cax.cax_df(
            cax_fn, cax_datatype, [cax_ticker], [cax_fields], start_date, end_date,
            options=cax_filter)
        return data_df

    def convert_cax_df_to_vol_surface(self, data_df, date):

        if data_df.empty:
            return None

        spot = float(data_df.Spot.unique()[0])

        data_df['Maturity_Date'] = pd.to_datetime(data_df.Maturity)

        data_df['term'] = data_df.apply(
            lambda row: float((row['Maturity_Date'] - row['Date']).days) / 365.0, axis=1)

        terms = sorted(data_df['term'].tolist())

        fwd_factors = data_df['Forward Factor'].tolist()

        moneyness_col = [col for col in data_df.columns if col.replace('.', '').isdigit()]
        strikes = np.asarray([float(mon) * spot for mon in moneyness_col])

        smiles = []
        for item in range(len(terms)):
            vols = data_df[data_df.term == terms[item]][moneyness_col].iloc[0].values
            newvolsfunc = intrp.interp1d(x=strikes, y=vols,
                                         bounds_error=False, fill_value=(
                    vols[0], vols[-1]))
            fwd = float(data_df[data_df.term == terms[item]]['Forward'].unique()[0])
            fwdstrikes = [mon * fwd for mon in self.fwd_money]
            fwdvols = newvolsfunc(fwdstrikes)
            smiles.append(qvs.EquityStrikeInterpSmile(self.fwd_money, fwdvols, fwd))

        return qvs.EquityVolSurface(date, terms, smiles, fwd_factors, spot)

    @cache_response(cache_vol_surf_name, cache_vol_surf_region_temp, True)
    @cache_response(cache_vol_surf_name, cache_vol_surf_region_perm, True)
    def get_vol_surface(self, id, date):

        data_df = self.get_cax_df(id, date, date)
        return self.convert_cax_df_to_vol_surface(data_df, date)

    def bulk_load_vol_surfaces(self, id, date_list):

        start_date = min(date_list)
        end_date = max(date_list)

        data_dfs = self.get_cax_df(id, start_date, end_date)
        trading_date_list = set(data_dfs['Date'])

        for d in trading_date_list:
            data_df = data_dfs[data_dfs['Date'] == d]
            vol_surface = self.convert_cax_df_to_vol_surface(data_df, d)
            cache_put(self.cache_vol_surf_name, self.cache_vol_surf_region_temp, vol_surface, id, d)
            cache_put(self.cache_vol_surf_name, self.cache_vol_surf_region_perm, vol_surface, id, d)


class IrVolCubeLoader(BaseLoader):
    '''
    Base class for loading ir volcubes
    '''

    cache_vol_cube_name = 'IrVolCubeLoader.get_vol_cube'
    cache_vol_cube_region_temp = 'mem_10h'
    cache_vol_cube_region_perm = 'disk'

    def __init__(self):
        super().__init__()

    def get_vol_cube(self, curve_handle, date):
        raise NotImplementedError

    def bulk_load_vol_cubes(self, curve_handle, date_list):
        raise NotImplementedError

    def clear_vol_cube_temp_cache(self):
        clear_cache(self.cache_vol_cube_name, self.cache_vol_cube_region_temp)

    def clear_vol_cube_perm_cache(self):
        clear_cache(self.cache_vol_cube_name, self.cache_vol_cube_region_perm)


class IrVolCubeLoaderStatic(IrVolCubeLoader):
    '''
    Class for loading volcubes from explicit input data
    '''

    def __init__(
            self, holiday_oracle=qau.Holidays()
    ):
        super().__init__()
        self.holiday_oracle = holiday_oracle
        self.cache = {}

    def get_vol_cube(self, curve_handle, date):
        return self.cache.get((curve_handle, date), None)

    def cache_vol_cube(
            self, curve_handle, date, expiry_list, tenor_list,
            vol_grid, beta_grid, rho_grid, volvol_grid, shift_grid
    ):
        res = qau.swaptions_exp_and_tenor_ycf(date, curve_handle, self.holiday_oracle, expiry_list, tenor_list)
        expiry_bus251_ycfs = res['exp_ycfs']
        tenors = res['tenor_ycfs']

        self.cache[(curve_handle, date)] = qvs.SabrIRVolCube(
            date, curve_handle, self.holiday_oracle, expiry_bus251_ycfs, tenors, vol_grid,
            beta_grid, rho_grid, volvol_grid, shift_grid
        )


class IrVolCubeLoaderOpenData(IrVolCubeLoader):
    '''
    Class for loading ir volcubes from opendata
    '''
    cache_vol_cube_name = 'IrVolCubeLoaderOpenData.get_vol_cube'
    cache_vol_cube_region_temp = 'mem_10h'
    cache_vol_cube_region_perm = 'disk'

    def __init__(
            self, holiday_oracle, fit_name='barc_fit'
    ):
        super().__init__()
        self.holiday_oracle = holiday_oracle
        self.fit_name = fit_name

    def reorder_and_merge_data_df(self, merge_df, data_df, item_name, tenor_list, expiry_list):
        data_grid_df = merge_df.set_index(item_name).join(
            data_df.set_index('item')).reset_index().drop(
            [item_name, 'ref_item', 'cut', 'source', 'attribute', 'timestamp'], axis=1
        ).pivot(index='exp', columns='ten', values='value')
        return data_grid_df[tenor_list].reindex(expiry_list)

    def vol_conversion_factor(self, od_swtn_str, asof_date, curve_handle, holidays, convert_to_bus251=True):
        expiry_str = od_swtn_str.split('_')[-4]
        expiry_date = qau.expiry_date_from_trade_date(asof_date, expiry_str, curve_handle, holidays)
        bus_tte = qau.bus_ycf(asof_date, expiry_date, curve_handle, holidays)
        act_tte = qau.ac_day_cnt_frac(asof_date, expiry_date, 'ACT/365F')

        if convert_to_bus251:
            return math.sqrt(act_tte / bus_tte)
        else:
            return math.sqrt(bus_tte / act_tte)

    @cache_response(cache_vol_cube_name, cache_vol_cube_region_temp, True)
    @cache_response(cache_vol_cube_name, cache_vol_cube_region_perm, True)
    def get_vol_cube(self, curve_handle, date, extrap_sabr=False, atm_grid_source='icap'):
        swtn_prefix = odc.OD_SWAPTION_CORE_DICT[curve_handle]['item_prefix']
        cut = odc.OD_SWAPTION_CORE_DICT[curve_handle]['cut']
        holiday_oracle = qau.Holidays()
        if atm_grid_source == 'barcap_rates_grp_clean':
            expiry_list = odc.BARCAP_SWAPTION_FILE_DICT[curve_handle]['expiries']
            tenor_list_raw = odc.BARCAP_SWAPTION_FILE_DICT[curve_handle]['tenors']
            vol_attr = 'normvol_bus251'
       else:
            expiry_list = odc.ICAP_SWAPTION_GRID_DICT[curve_handle]['expiries']
            tenor_list_raw = odc.ICAP_SWAPTION_GRID_DICT[curve_handle]['tenors']
            vol_attr = 'nvol'  # Convert this to bizday vol below

        tenor_list = []
        for t in tenor_list_raw:
            if t[-1].lower() == 'y':
                tenor_list.append(t)

        res = qau.swaptions_exp_and_tenor_ycf(date, curve_handle, holiday_oracle, expiry_list, tenor_list)
        expiry_bus251_ycfs = res['exp_ycfs']
        tenors = res['tenor_ycfs']

        otl_vol = []
        otl_beta = []
        otl_rho = []
        otl_volvol = []
        otl_shift = []

        merge_data_list = []
        for expiry in expiry_list:
            for tenor in tenor_list:
                vol_item = '_'.join([swtn_prefix, expiry, '0d', tenor, 'ao0'])
                ref_item = '_'.join([swtn_prefix, expiry, '0d', tenor, 'ref'])
                merge_data_list.append([expiry, tenor, vol_item, ref_item])
                otl_vol.append((vol_item, vol_attr, cut, atm_grid_source))
                otl_beta.append((ref_item, 'beta_%s' % self.fit_name, cut, 'rates_grp_clean'))
                otl_rho.append((ref_item, 'rho_%s' % self.fit_name, cut, 'rates_grp_clean'))
                otl_volvol.append((ref_item, 'volvol_%s' % self.fit_name, cut, 'rates_grp_clean'))
                otl_shift.append((ref_item, 'shift_%s' % self.fit_name, cut, 'rates_grp_clean'))

        if extrap_sabr:
            extrap_expiry_list = [x for x in ['15y', '20y', '30y'] if x not in expiry_list]
            full_expiry_list = expiry_list + extrap_expiry_list
            for expiry in extrap_expiry_list:

                exp_date = qau.expiry_date_from_trade_date(date, expiry, curve_handle, holiday_oracle)
                expiry_bus251_ycfs.append(qau.bus_ycf(date, exp_date, curve_handle, holiday_oracle))

                for tenor in tenor_list:
                    vol_item = '_'.join([swtn_prefix, expiry, '0d', tenor, 'ao0'])
                    merge_data_list.append([expiry, tenor, vol_item, ''])
                    otl_vol.append((vol_item, 'nvol', cut, 'icap'))

        merge_data_df = pd.DataFrame(merge_data_list[:len(expiry_list) * len(tenor_list)],
                                     columns=['exp', 'ten', 'vol_item', 'ref_item'])

        # TODO: This should be one DB hit
        # Get OD data
        vol_df_raw = od.df_for_observable_tuples(
            otl_vol, start_date=date, end_date=date, return_records=True
        )
        # Convert busday vols
        vol_df_raw.loc[:, 'value'] = vol_df_raw.apply(
            lambda row: self.vol_conversion_factor(
                row['item'], date, curve_handle, holiday_oracle) * row['value']
            if row['source'] == 'icap' else row['value'], axis=1)
        beta_df_raw = od.df_for_observable_tuples(
            otl_beta, start_date=date, end_date=date, return_records=True
        )
        rho_df_raw = od.df_for_observable_tuples(
            otl_rho, start_date=date, end_date=date, return_records=True
        )
        volvol_df_raw = od.df_for_observable_tuples(
            otl_volvol, start_date=date, end_date=date, return_records=True
        )
        shift_df_raw = od.df_for_observable_tuples(
            otl_shift, start_date=date, end_date=date, return_records=True
        )

        # Reformat data
        if extrap_sabr:
            merge_vol_data_df = pd.DataFrame(merge_data_list, columns=['exp', 'ten', 'vol_item', 'ref_item'])
            vol_grid_df = self.reorder_and_merge_data_df(
                merge_df=merge_vol_data_df, data_df=vol_df_raw, item_name='vol_item', tenor_list=tenor_list,
                expiry_list=full_expiry_list
            )
            # interpolating the missing 12yr tenor for long-dated swaptions from icap vols
            for expiry in extrap_expiry_list:
                good_tenors = [x for i, x in enumerate(tenors) if i != tenor_list.index('12y')]
                good_vols = vol_grid_df.loc[expiry, vol_grid_df.columns != '12y'].tolist()
                vol_grid_df.loc[expiry, '12y'] = np.interp(tenors[tenor_list.index('12y')],
                                                           good_tenors, good_vols)

        else:
            vol_grid_df = self.reorder_and_merge_data_df(
                merge_df=merge_data_df, data_df=vol_df_raw, item_name='vol_item', tenor_list=tenor_list,
                expiry_list=expiry_list
            )

        beta_grid_df = self.reorder_and_merge_data_df(
            merge_df=merge_data_df, data_df=beta_df_raw, item_name='ref_item', tenor_list=tenor_list,
            expiry_list=expiry_list
        )
        rho_grid_df = self.reorder_and_merge_data_df(
            merge_df=merge_data_df, data_df=rho_df_raw, item_name='ref_item', tenor_list=tenor_list,
            expiry_list=expiry_list
        )
        volvol_grid_df = self.reorder_and_merge_data_df(
            merge_df=merge_data_df, data_df=volvol_df_raw, item_name='ref_item', tenor_list=tenor_list,
            expiry_list=expiry_list
        )
        shift_grid_df = self.reorder_and_merge_data_df(
            merge_df=merge_data_df, data_df=shift_df_raw, item_name='ref_item', tenor_list=tenor_list,
            expiry_list=expiry_list
        )

        if extrap_sabr:
            expiry_dict = dict(zip(list(range(0, len(full_expiry_list))), full_expiry_list))

            beta_grid_df = beta_grid_df.append([beta_grid_df[-1:]] * len(extrap_expiry_list), ignore_index=True)
            beta_grid_df.index = beta_grid_df.index.to_series().map(expiry_dict)

            rho_grid_df = rho_grid_df.append([rho_grid_df[-1:]] * len(extrap_expiry_list), ignore_index=True)
            rho_grid_df.index = rho_grid_df.index.to_series().map(expiry_dict)

            volvol_grid_df = volvol_grid_df.append([volvol_grid_df[-1:]] * len(extrap_expiry_list), ignore_index=True)
            volvol_grid_df.index = volvol_grid_df.index.to_series().map(expiry_dict)

            shift_grid_df = shift_grid_df.append([shift_grid_df[-1:]] * len(extrap_expiry_list), ignore_index=True)
            shift_grid_df.index = shift_grid_df.index.to_series().map(expiry_dict)

        return qvs.SabrIRVolCube(
            date, curve_handle, holiday_oracle, expiry_bus251_ycfs, tenors, vol_grid_df.values,
            beta_grid_df.values, rho_grid_df.values, volvol_grid_df.values, shift_grid_df.values
        )

    def bulk_load_vol_cubes(self, curve_handle, date_list, write_perm=True):
        '''
        :param date_list: [start_date, end_date]
        :param curve_handle: str like USD.3ML, UR.6ML, GBP.6ML
        :param bool write_perm: cache to disk as well as memory?
        :return: dictionary of _vol cubes
        '''
        holiday_oracle = qau.Holidays()

        start_date = min(date_list)
        end_date = max(date_list)
        trading_date_list = date_list

        swtn_prefix = odc.OD_SWAPTION_CORE_DICT[curve_handle]['item_prefix']
        cut = odc.OD_SWAPTION_CORE_DICT[curve_handle]['cut']

        expiry_list = odc.ICAP_SWAPTION_GRID_DICT[curve_handle]['expiries']
        tenor_list_raw = odc.ICAP_SWAPTION_GRID_DICT[curve_handle]['tenors']

        tenor_list = []
        for t in tenor_list_raw:
            if t[-1].lower() == 'y':
                tenor_list.append(t.lower())

        otl_vol = []
        otl_beta = []
        otl_rho = []
        otl_volvol = []
        otl_shift = []
        merge_data_list = []
        for expiry in expiry_list:
            for tenor in tenor_list:
                vol_item = '_'.join([swtn_prefix, expiry, '0d', tenor, 'ao0'])
                ref_item = '_'.join([swtn_prefix, expiry, '0d', tenor, 'ref'])
                merge_data_list.append([expiry, tenor, vol_item, ref_item])
                # Pull calday vol and adjust farther down.
                otl_vol.append((vol_item, 'nvol', cut, 'icap'))
                otl_beta.append((ref_item, 'beta_%s' % self.fit_name, cut, 'rates_grp_clean'))
                otl_rho.append((ref_item, 'rho_%s' % self.fit_name, cut, 'rates_grp_clean'))
                otl_volvol.append((ref_item, 'volvol_%s' % self.fit_name, cut, 'rates_grp_clean'))
                otl_shift.append((ref_item, 'shift_%s' % self.fit_name, cut, 'rates_grp_clean'))

        merge_data_df = pd.DataFrame(merge_data_list, columns=['exp', 'ten', 'vol_item', 'ref_item'])

        ## TODO: this should be one db hit
        vol_df_raw = od.df_for_observable_tuples(otl_vol, start_date, end_date, return_records=True)
        # Adjust calday vols to bizday
        vol_df_raw.loc[:, 'value'] = vol_df_raw.apply(
            lambda row: self.vol_conversion_factor(
                row['item'], row['timestamp'].date(), curve_handle, holiday_oracle) * row['value'],
            axis=1)

        beta_df_raw = od.df_for_observable_tuples(otl_beta, start_date, end_date, return_records=True)
        rho_df_raw = od.df_for_observable_tuples(otl_rho, start_date, end_date, return_records=True)
        volvol_df_raw = od.df_for_observable_tuples(otl_volvol, start_date, end_date, return_records=True)
        shift_df_raw = od.df_for_observable_tuples(otl_shift, start_date, end_date, return_records=True)

        for d in trading_date_list:
            res = qau.swaptions_exp_and_tenor_ycf(d, curve_handle, self.holiday_oracle, expiry_list, tenor_list)
            expiry_ycfs = res['exp_ycfs']
            tenor_ycfs = res['tenor_ycfs']
            vol_grid = self.reorder_and_merge_data_df(
                merge_data_df, vol_df_raw[vol_df_raw['timestamp'] == pd.to_datetime(d, utc=True)], 'vol_item',
                tenor_list, expiry_list
            ).values
            beta_grid = self.reorder_and_merge_data_df(
                merge_data_df, beta_df_raw[beta_df_raw['timestamp'] == pd.to_datetime(d, utc=True)],
                'ref_item', tenor_list, expiry_list
            ).values
            rho_grid = self.reorder_and_merge_data_df(
                merge_data_df, rho_df_raw[rho_df_raw['timestamp'] == pd.to_datetime(d, utc=True)], 'ref_item',
                tenor_list, expiry_list
            ).values
            volvol_grid = self.reorder_and_merge_data_df(
                merge_data_df, volvol_df_raw[volvol_df_raw['timestamp'] == pd.to_datetime(d, utc=True)], 'ref_item',
                tenor_list, expiry_list
            ).values
            shift_grid = self.reorder_and_merge_data_df(
                merge_data_df, shift_df_raw[shift_df_raw['timestamp'] == pd.to_datetime(d, utc=True)], 'ref_item',
                tenor_list, expiry_list
            ).values
            vs = qvs.SabrIRVolCube(
                d, curve_handle, self.holiday_oracle, expiry_ycfs, tenor_ycfs, vol_grid, beta_grid, rho_grid,
                volvol_grid, shift_grid
            )
            # insert into cache such that we mimic the single loader (get_vol_cube).
            cache_put(self.cache_vol_cube_name, self.cache_vol_cube_region_temp, vs, curve_handle, d)
            if write_perm:
                cache_put(self.cache_vol_cube_name, self.cache_vol_cube_region_perm, vs, curve_handle, d)

    def get_vols(self, curve_handle, date_list, fwd_list, strike_list, exp_date_list, tenor_list):
        '''
        All list arguments must have the same length!
        '''
        return [
            self.get_vol_cube(curve_handle, date).get_vol(fwd, stk, exp_date, tenor)
            for date, fwd, stk, exp_date, tenor in zip(date_list, fwd_list, strike_list, exp_date_list, tenor_list)
        ]

    def cache_vol_cube(self, vol_cube, curve_handle, date):
        cache_put(self.cache_vol_cube_name, self.cache_vol_cube_region_temp, vol_cube, curve_handle, date)
        cache_put(self.cache_vol_cube_name, self.cache_vol_cube_region_perm, vol_cube, curve_handle, date)

    def temp_cache_vol_cube(self, vol_cube, curve_handle, date):
        cache_put(self.cache_vol_cube_name, self.cache_vol_cube_region_temp, vol_cube, curve_handle, date)


class CurveLoader(BaseLoader):
    '''
    Base class for loading interest rate curves
    '''

    def __init__(self):
        super().__init__()

    def get_curve(self, curve_name, date):
        raise NotImplementedError

    def get_curves(self, curve_names, date_list):
        df = pd.DataFrame({
            cn: {
                date: self.get_curve(cn, date)
                for date in date_list
            }
            for cn in curve_names
        })
        return df.reindex(index=date_list, columns=curve_names)

    def bulk_load_curves(self, curve_name_list, date_list):
        raise NotImplementedError


class CurveLoaderAlib(CurveLoader):
    '''
    Curve loader for ALIB curves from the file system
    '''
    cache_curve_name = 'CurveLoaderAlib.get_curve'
    cache_curve_region = 'mem_10h'

    def __init__(self, curves_location=ALIB_CURVE_REGION, live_mode=False):
        """
        :param str curves_location: root path with serialized curves in dated subdirectories.
        :param bool live_mode: is curves_location a live-ticking curves directory? \
        When live, curves are in root of curves_location.
        """
        super().__init__()
        self.live_mode = live_mode
        self.curve_base_dir = blu.get_curve_dir_from_loc(curves_location)
        self.cache = cache.get_cache_region(self.cache_curve_name, self.cache_curve_region)

    @cache_response(cache_curve_name, cache_curve_region, True)
    def get_curve(self, curve_name, date):
        return qau.pull_stored_zc(curve_name, date, self.curve_base_dir, self.live_mode)

    # TODO: rework this to performs a faster kind of load and directly put into cache
    def bulk_load_curves(self, curve_name_list, date_list):
        for curve_name in curve_name_list:
            for date in date_list:
                self.get_curve(curve_name, date)

    def cache_curve(self, curve, curve_name, date):
        cache_put(self.cache_curve_name, self.cache_curve_region, curve, curve_name, date)


class FxSpotLoader(BaseLoader):
    '''
    Base loader for FX spots
    '''

    def __init__(self):
        super().__init__()

    def get_spot(self, spot_name, date):
        raise NotImplementedError

    def get_spots(self, spot_name_list, date_list):
        df = pd.DataFrame({
            sn: {
                date: self.get_spot(sn, date)
                for date in date_list
            }
            for sn in spot_name_list
        })
        return df.reindex(index=date_list, columns=spot_name_list)

    def bulk_load_spots(self, spot_name_list, date_list):
        raise NotImplementedError


class FxSpotLoaderOpenData(FxSpotLoader):
    '''
    Loader for FX spots from opendata
    '''
    cache_spot_name = 'FxSpotLoaderOpenData.get_spot'
    cache_spot_region_temp = 'mem_10h'
    cache_spot_region_perm = 'disk'

    cache_spot_1600_name = 'FxSpotLoaderOpenData.get_spot_1600'

    def __init__(self):
        super().__init__()
        self.cache = cache.get_cache_region(self.cache_spot_name, self.cache_spot_region_temp)

    def spot_name_to_od_ticker(self, spot_name):
        return 'fx_linear_%s_sp|outright|nyclose|ph' % spot_name.lower()

    def spot_name_to_od_item(self, spot_name):
        return 'fx_linear_%s_sp' % spot_name.lower()

    def spot_name_to_od_ticker_1600(self, spot_name, is_1600, fxois_config_dict):
        if is_1600:
            return 'fx_linear_%s_sp|outright|lonclose_1600|fo_snap' % spot_name.lower()
        else:
            ticker = 'fx_linear_%s_sp|outright|'  % spot_name.lower()
            cut = fxois_config_dict[spot_name].get('snapcut','nyclose')

            return ticker + cut + '|qa_grp_snap'


    @cache_response(cache_spot_name, cache_spot_region_temp, True)
    @cache_response(cache_spot_name, cache_spot_region_perm, True)
    def get_spot(self, spot_name, date):
        ticker = self.spot_name_to_od_ticker(spot_name)
        data_df = od.df_for_observable_strings(
            [ticker], start_date=date, end_date=date, return_records=False)

        if len(data_df) == 1:
            return data_df.iloc[0, 0]
        else:
            return float('NaN')

    @cache_response(cache_spot_1600_name, cache_spot_region_temp, True)
    @cache_response(cache_spot_1600_name, cache_spot_region_perm, True)
    def get_spot_1600(self, spot_name, date, fxois_config_dict):
        # RL: Not nice.. but we started snapping the ldn close fx on this date
        if date < dt.date(2019, 8, 30):
            ticker = self.spot_name_to_od_ticker_1600(spot_name, False, fxois_config_dict)
        else:
            ticker = self.spot_name_to_od_ticker_1600(spot_name, True, fxois_config_dict)

        data_df = od.df_for_observable_strings(
            [ticker], start_date=date, end_date=date, return_records=False)

        if len(data_df) == 1:
            return data_df.iloc[0, 0]
        else:
            return float('NaN')

    def clear_spot_fx_temp_cache(self):
        clear_cache(self.cache_spot_name, self.cache_spot_region_temp)

    def clear_spot_fx_perm_cache(self):
        clear_cache(self.cache_spot_name, self.cache_spot_region_perm)

    def bulk_load_spots(self, spot_name_list, date_list):
        start_date = min(date_list)
        end_date = max(date_list)
        tickers = [self.spot_name_to_od_ticker(s) for s in spot_name_list]
        ticker_map = {self.spot_name_to_od_item(s): s for s in spot_name_list}
        data_df = od.df_for_observable_strings(
            tickers, start_date=start_date, end_date=end_date, return_records=True)
        data_df.drop(['attribute', 'cut', 'source'], axis=1, inplace=True)
        data_df['item'] = [ticker_map[i] for i in data_df['item']]
        data_df['timestamp'] = [dt.date(t.year, t.month, t.day) for t in data_df['timestamp']]

        for ix, ser in data_df.iterrows():
            cache_put(self.cache_spot_name, self.cache_spot_region_temp,
                      ser.loc['value'], ser.loc['item'], ser.loc['timestamp'])
            cache_put(self.cache_spot_name, self.cache_spot_region_perm,
                      ser.loc['value'], ser.loc['item'], ser.loc['timestamp'])

    def cache_spot(self, spot, spot_name, date):
        cache_put(self.cache_spot_name, self.cache_spot_region_temp, spot, spot_name, date)
        cache_put(self.cache_spot_name, self.cache_spot_region_perm, spot, spot_name, date)

    def temp_cache_spot(self, spot, spot_name, date):
        cache_put(self.cache_spot_name, self.cache_spot_region_temp, spot, spot_name, date)


class AssetDataLoader(BaseLoader):
    '''
    Under construction.
    '''

    def __init__(self):
        super().__init__()
        self.data_dict = {}

    def result_in_store(self, asset_name, property, asof_date):
        try:
            self.data_dict[asset_name][property].loc[asof_date]
            return True
        except KeyError:
            return False

    def get_result(self, asset_name, property, asof_date):
        return self.data_dict[asset_name][property][asof_date]

    def store_result(self, asset_name, property, asof_date, result):
        if asset_name not in self.data_dict:
            self.data_dict[asset_name] = {}
        if property not in self.data_dict[asset_name]:
            self.data_dict[asset_name][property] = pd.Series()
        self.data_dict[asset_name][property].loc[asof_date] = result