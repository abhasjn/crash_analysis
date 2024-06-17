from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql.functions import col, sum, row_number

from utils.io import save_df


class CarCrashAnalysis:
    def __init__(self, charges_df: DataFrame, damages_df: DataFrame, endorse_df: DataFrame,
                 primary_person_df: DataFrame, restrict_df: DataFrame, units_df: DataFrame):
        """
        Initialize the Analysis class with DataFrames.

        """
        self.charges_df = charges_df
        self.damages_df = damages_df
        self.endorse_df = endorse_df
        self.primary_person_df = primary_person_df
        self.restrict_df = restrict_df
        self.units_df = units_df

    def analysis_1(self):
        """
        Analysis 1: Find the number of crashes (accidents) in which number of males killed are greater than 2.

        Returns:
        int: The number of crashes with more than 2 males killed.
        """
        desc = "The number of crashes (accidents) in which number of males killed are greater than 2 is :"
        filtered_df = self.primary_person_df.filter(
            (col('PRSN_GNDR_ID') == 'MALE') &
            (col("PRSN_INJRY_SEV_ID") == 'KILLED')
        )

        result = filtered_df.groupBy('CRASH_ID').count().filter(col('count') > 2).count()

        return desc, result

    def analysis_2(self):
        """
        Perform Analysis 2: How many two-wheelers are booked for crashes?

        Returns:
        int: The count of two-wheelers booked for crashes.
        """
        desc = "The number of two-wheelers booked for crashes (accidents): "
        two_wheeler_types = ['MOTORCYCLE', 'POLICE MOTORCYCLE']

        result = self.units_df.filter((col('VEH_BODY_STYL_ID').isin(two_wheeler_types)) | (
            col('VEH_BODY_STYL_ID').contains('MOTORCYCLE'))).count()
        return desc, result

    def analysis_3(self):
        """
        Perform Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.

        Returns:
        DataFrame: A DataFrame with the top 5 vehicle makes.
        """
        desc = "The Top 5 Vehicles make of the cars present in the crashes in which a driver died and Airbags did not deploy is : "

        filtered_primary_person_df = self.primary_person_df.filter(
            (col('PRSN_TYPE_ID') == 'DRIVER') &
            (col('PRSN_INJRY_SEV_ID') == 'KILLED') &
            (col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED')
        )

        filtered_units_df = self.units_df.filter(~col('VEH_MAKE_ID').isin(['OTHER (EXPLAIN IN NARRATIVE)', 'NA']))

        # Join with units data
        # Including both CRASH_ID and UNIT_NBR in the join condition is necessary because CRASH_ID and UNIT_NBR together uniquely identify each row in the units_df DataFrame. 
        # This is because multiple units (vehicles) can be involved in the same crash, so a simple join on CRASH_ID alone would not be sufficient to distinguish between different vehicles within the same crash.
        joined_df = filtered_primary_person_df.join(
            filtered_units_df,
            ['CRASH_ID', 'UNIT_NBR']
        )

        result = joined_df.groupBy('VEH_MAKE_ID').count().orderBy(col('count').desc()).limit(5)

        return desc, result

    def analysis_4(self):
        """
        Perform Analysis 4: Determine the number of vehicles with drivers having valid licenses involved in hit-and-run.

        Returns:
        int: The count of such vehicles.
        """
        desc = "The number of Vehicles with a driver having valid licences involved in hit-and-run is:"

        valid_license_types = ['DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.']

        valid_license_df = self.primary_person_df.filter(
            col('DRVR_LIC_TYPE_ID').isin(valid_license_types)
        )

        # Filter units_df for hit and run 
        hit_and_run_df = self.units_df.filter(col('VEH_HNR_FL') == 'Y')

        joined_df = valid_license_df.join(hit_and_run_df, ['CRASH_ID', 'UNIT_NBR'])

        result = joined_df.dropDuplicates(['CRASH_ID', 'UNIT_NBR']).count()

        return desc, result

    def analysis_5(self):
        """
        Perform Analysis 5: Determine the state with the highest number of accidents in which females are not involved.

        Returns:
        DataFrame: A DataFrame with the state having the highest number of such accidents.
        """
        desc = "The state has the highest number of accidents in which females are not involved is: "

        filtered_df = self.primary_person_df.filter(col('PRSN_GNDR_ID') != 'FEMALE')

        filtered_df = filtered_df.filter(~col('DRVR_LIC_STATE_ID').isin(['Other']))

        result = filtered_df.groupBy('DRVR_LIC_STATE_ID').count().orderBy(col('count').desc()).limit(1)
        return desc, result

    def analysis_6(self):
        """
        Perform Analysis 6: Determine the Top 3rd to 5th VEH_MAKE_IDs that contribute to the largest number of
        injuries including death.

        Returns:
        DataFrame: A DataFrame with the Top 3rd to 5th VEH_MAKE_IDs.

        """
        desc = "The Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death is :"

        # Create a new column 'TOTAL_INJRY_AND_DEATH' by summing 'TOT_INJRY_CNT' and 'DEATH_CNT'
        self.units_df = self.units_df.withColumn('TOTAL_INJRY_AND_DEATH', col('TOT_INJRY_CNT') + col('DEATH_CNT'))

        filtered_units_df = self.units_df.filter(~col('VEH_MAKE_ID').isin(['OTHER (EXPLAIN IN NARRATIVE)', 'NA']))

        # Group by 'VEH_MAKE_ID' and sum the 'TOTAL_INJRY_AND_DEATH' column, then order by the sum in descending order
        result = filtered_units_df.groupBy('VEH_MAKE_ID') \
            .agg(sum('TOTAL_INJRY_AND_DEATH').alias('TOTAL_INJRY_AND_DEATH_SUM')) \
            .orderBy(col('TOTAL_INJRY_AND_DEATH_SUM').desc())

        window_spec = Window.orderBy(col('TOTAL_INJRY_AND_DEATH_SUM').desc())

        result_with_row_number = result.withColumn("row_number", row_number().over(window_spec))

        final_result = result_with_row_number.filter((col("row_number") >= 3) & (col("row_number") <= 5)).drop(
            "row_number")

        return desc, final_result

    def analysis_7(self):
        """
        Perform Analysis 7: For all body styles involved in crashes, mention the top ethnic user group of each unique
        body style.

        Returns:
        DataFrame: A DataFrame with each body style and its top ethnic user group.
        """
        desc = "For all the body styles involved in crashes, the top ethnic user group of each unique body style is: "

        # Data Cleaning based on the problem Statement.
        filtered_primary_person_df = self.primary_person_df.filter(~col("PRSN_ETHNICITY_ID").isin(["NA", "UNKNOWN"]))
        filtered_units_df = self.units_df.filter(
            ~col("VEH_BODY_STYL_ID").isin(["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"]))

        joined_df = filtered_primary_person_df.join(filtered_units_df, ['CRASH_ID', 'UNIT_NBR'])

        body_style_ethnic_group_df = joined_df.groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').count()

        window_spec = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('count').desc())

        # Adding a rank based on count within each partition of VEH_BODY_STYL_ID
        ranked_df = body_style_ethnic_group_df.withColumn('rank', row_number().over(window_spec))

        # Filtering to get the top ethnic user group (rank == 1) for each body style
        result = ranked_df.filter(col('rank') == 1).drop('rank')

        return desc, result

    def analysis_8(self):
        """
        Perform Analysis 8: Among the crashed cars, determine the Top 5 Zip Codes with the highest number of crashes
        with alcohol as a contributing factor (Use Driver Zip Code).

        Returns:
        DataFrame: A DataFrame with the Top 5 Zip Codes.
        """
        desc = "Among the crashed cars, the Top 5 Zip Codes with the highest number of crashes with alcohol as the contributing factor to a crash is:"

        alcohol_conditions = [
            'UNDER INFLUENCE - ALCOHOL',
            'HAD BEEN DRINKING'
        ]

        filtered_df = self.units_df.filter(
            col('CONTRIB_FACTR_1_ID').isin(alcohol_conditions) |
            col('CONTRIB_FACTR_2_ID').isin(alcohol_conditions) |
            col('CONTRIB_FACTR_P1_ID').isin(alcohol_conditions)
        )

        joined_df = filtered_df.join(self.primary_person_df, ['CRASH_ID', 'UNIT_NBR'])

        result = joined_df.filter(col('DRVR_ZIP').isNotNull()).groupBy('DRVR_ZIP').count().orderBy(
            col("count").desc()).limit(5)
        return desc, result

    def analysis_9(self):
        """
        Perform Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed,
        Damage Level (VEH_DMAG_SCL~) is above 4, and the car avails Insurance.

        Returns:
        int: the count of such distinct Crash IDs.
        """
        desc = "Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance is:"

        damages_joined_df = self.damages_df.join(self.units_df, on=['CRASH_ID'])

        damage_greater_than_4 = ['DAMAGED 6', 'DAMAGED 5', 'DAMAGED 7 HIGHEST']
        insurance_availed = ['PROOF OF LIABILITY INSURANCE', 'LIABILITY INSURANCE POLICY', 'INSURANCE BINDER']

        result = damages_joined_df.filter((col('VEH_DMAG_SCL_1_ID').isin(damage_greater_than_4)) |
                                          (col('VEH_DMAG_SCL_2_ID').isin(damage_greater_than_4))) \
            .filter(col('DAMAGED_PROPERTY') == 'NONE') \
            .filter(col('FIN_RESP_TYPE_ID').isin(insurance_availed)).count()

        return desc, result

    def analysis_10(self):
        """
        Perform Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related
        offences, have licensed Drivers, use top 10 used vehicle colours, and have cars licensed with the Top 25
        states with the highest number of offences.

        Returns:
        DataFrame: A DataFrame with the Top 5 Vehicle Makes.
        """
        desc = "The Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offenses is:"

        valid_license_types = ['DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.']

        top_25_states_df = self.units_df.groupby('VEH_LIC_STATE_ID').count().orderBy(col('count').desc()).limit(25)
        top_25_state_list = [row[0] for row in
                             top_25_states_df.filter(col('VEH_LIC_STATE_ID').cast('int').isNull()).collect()]

        top_10_vehicle_colors_df = self.units_df.groupby('VEH_COLOR_ID').count().orderBy(col('count').desc()).limit(10)
        top_10_used_vehicle_colors = [row[0] for row in top_10_vehicle_colors_df.filter(
            col('VEH_COLOR_ID').cast('int').isNull()).collect()]

        joined_df = self.charges_df.join(self.primary_person_df, on=['CRASH_ID']).join(self.units_df, on=['CRASH_ID'])

        result = joined_df.filter(col('CHARGE').contains('SPEED')) \
            .filter(col('DRVR_LIC_TYPE_ID').isin(valid_license_types)) \
            .filter(col('VEH_COLOR_ID').isin(top_10_used_vehicle_colors)) \
            .filter(col('VEH_LIC_STATE_ID').isin(top_25_state_list)) \
            .groupby("VEH_MAKE_ID").count().orderBy(col("count").desc()).limit(5)

        return desc, result

    def add_analysis(self, analysis_count, result, output_config):
        analysis_dict = {
            1: self.analysis_1(),
            2: self.analysis_2(),
            3: self.analysis_3(),
            4: self.analysis_4(),
            5: self.analysis_5(),
            6: self.analysis_6(),
            7: self.analysis_7(),
            8: self.analysis_8(),
            9: self.analysis_9(),
            10: self.analysis_10()
        }
        analysis_desc, analysis_result = analysis_dict[analysis_count]
        print(analysis_desc, analysis_result)
        if isinstance(analysis_result, DataFrame):
            analysis_result.show()
            save_df(analysis_result, output_config[f"analysis_result_{analysis_count}"])
        elif isinstance(analysis_result, int):
            result.append({'Analysis No.': analysis_count, 'description': analysis_desc, 'value': analysis_result})
