import json
import os
from datetime import datetime, timedelta
import requests

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from jinja2 import Environment, FileSystemLoader


#----------------- DAG--------------#
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 18),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG("postgres_data_load", default_args=default_args) as dag:

    task_1 = PostgresOperator(
        task_id = "create-table",
        database = "postgres",
        postgres_conn_id = "postgres_localhost",
        sql="""
        DROP TABLE IF EXISTS edw.edw_fbb_capex_qos_merged_percentile;

        CREATE TABLE edw.edw_fbb_capex_qos_merged_percentile (
            mo_key numeric(30) NULL,
            province varchar(50) NULL,
            province_name varchar(50) NULL,
            province_code varchar(50) NULL,
            district varchar(50) NULL,
            district_name varchar(50) NULL,
            district_code varchar(50) NULL,
            commune varchar(50) NULL,
            commune_name varchar(50) NULL,
            total_revenue numeric(30,6) NULL,
            olt_name varchar(50) NULL,
            opex_value_total_monthly numeric(30,6) NULL,
            opex_value_breakdown_electricity_monthly numeric(30,6) NULL,
            opex_value_breakdown_oper_maintenance_monthly numeric(30,6) NULL,
            opex_value_breakdown_rental_monthly numeric(30,6) NULL,
            opex_value_breakdown_labour_personnel_monthly numeric(30,6) NULL,
            opex_value_breakdown_repair_fixed_assets_monthly numeric(30,6) NULL,
            opex_value_breakdown_others_monthly numeric(30,6) NULL,
            ebidta numeric(30,6) NULL,
            ebidta_margin numeric(30,6) NULL,
            node_subscribers_ammount numeric(30,6) NULL,
            node_subscribers_capacity numeric(30,6) NULL,
            node_strategic_objective varchar(250) NULL,
            node_deployment_date varchar(250) NULL,
            node_equipment_lifetime varchar(250) NULL,
            node_total_capex_invested numeric(30,6) NULL,
            node_total_capex_invested_type varchar(100) NULL,
            capex_value_breakdown_civils_and_implementation numeric(30,6) NULL,
            capex_value_breakdown_centraloffice numeric(30,6) NULL,
            capex_value_breakdown_odn_hw numeric(30,6) NULL,
            capex_value_breakdown_fibre_cable numeric(30,6) NULL,
            capex_value_breakdown_end_user_hw numeric(30,6) NULL,
            capex_value_breakdown_software numeric(30,6) NULL,
            netbook_value_live_node numeric(30,6) NULL,
            area varchar NULL,
            area_code varchar(50) NULL,
            olt_type varchar(50) NULL,
            olt_id varchar(50) NULL,
            ip_address varchar(50) NULL,
            lat numeric(20,6) NULL,
            lon numeric(20,6) NULL,
            poi_id varchar(50) NULL,
            poi_name varchar(250) NULL,
            poi_address varchar(500) NULL,
            poi_category varchar(250) NULL,
            poi_category_ranking numeric(20) NULL,
            poi_size numeric(30,6) NULL,
            center_poi_lat numeric(20,6) NULL,
            center_poi_long numeric(20,6) NULL,
            cluster_id numeric(20) NULL,
            cluster_name varchar(250) NULL,
            cluster_type varchar(250) NULL,
            cluster_priority_category varchar(250) NULL,
            qos_mo numeric(30,6) NULL,
            qos_mpi numeric(30,6) NULL,
            qos_mci numeric(30,6) NULL,
            qos_md numeric(30,6) NULL,
            qos_mp numeric(30,6) NULL,
            qos_mo_indx numeric(30) NULL,
            qos_mpi_indx numeric(30) NULL,
            qos_mci_indx numeric(30) NULL,
            qos_md_indx numeric(30) NULL,
            qos_mp_indx numeric(30) NULL,
            total_revenue_area numeric(30,6) NULL,
            ebidta_area numeric(30,6) NULL,
            ebidta_margin_area numeric(30,6) NULL,
            total_revenue_province numeric(30,6) NULL,
            ebidta_province numeric(30,6) NULL,
            ebidta_margin_province numeric(30,6) NULL,
            total_revenue_district numeric(30,6) NULL,
            ebidta_district numeric(30,6) NULL,
            ebidta_margin_district numeric(30,6) NULL,
            total_revenue_cluster numeric(30,6) NULL,
            ebidta_cluster numeric(30,6) NULL,
            ebidta_margin_cluster numeric(30,6) NULL,
            total_revenue_poi numeric(30,6) NULL,
            ebidta_poi numeric(30,6) NULL,
            ebidta_margin_poi numeric(30,6) NULL,
            percentile_30 numeric(30,6) NULL,
            percentile_70 numeric(30,6) NULL,
            em_district_percentile_30 numeric(30,6) NULL,
            em_district_percentile_70 numeric(30,6) NULL,
            em_cluster_percentile_30 numeric(30,6) NULL,
            em_cluster_percentile_70 numeric(30,6) NULL,
            em_poi_percentile_30 numeric(30,6) NULL,
            em_poi_percentile_70 numeric(30,6) NULL,
            investment_zone_olt varchar(50) NULL,
            investment_zone_district varchar(50) NULL,
            investment_zone_cluster varchar(50) NULL,
            investment_zone_poi varchar(50) NULL,
            bw_utl_recom varchar(1000) NULL,
            ont_port_utl_recom varchar(1000) NULL,
            market_architype varchar(150) NULL,
            per_change_district numeric(30,6) NULL,
            per_change_cluster numeric(30,6) NULL,
            per_change_olt numeric(30,6) NULL,
            per_change_poi numeric(30,6) NULL
        );

        """
    )

    task_1