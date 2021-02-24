from pyspark.sql import SparkSession
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
#     # Use a breakpoint in the code line below to debug your script.
#     # Press Ctrl+F8 to toggle the breakpoint.

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('Application started...')

    spark = SparkSession \
            .builder \
            .appName('First PySpark Demo ..') \
            .master('local[*]') \
            .getOrCreate()

    #input_file_path = 'file:///C://Users/cheta//OneDrive//example.txt'
    #C:\Users\cheta\OneDrive\Desktop

    #tech_rdd = spark.sparkContext.textFile(input_file_path)
    corruptDF = spark.createDataFrame([(1, 'sam', 25), \
                                       (2, 'ram', None), \
                                       (None, None, None),
                                       (3, None, 35)
                                       ],
                                      ['id', 'name', 'bday'])

    print('Printing data in tech_rdd: ')

    corruptDF.show()

    print('Application completed')

