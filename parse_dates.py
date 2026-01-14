Tenho essa função, mas preciso de uma atualização:

    def _get_files_from_S3(
        self, file_type: str, date: str, path: str=BUCKET_PATH
    ) -> tuple[str, BytesIO, list[ZipInfo]]:
        """
        Retrieves a zip file from S3 based on the specified file type and date.

        Parameters:
        file_type (str): The type of the file to retrieve.
        date (str): The date string in the format 'YYYYMM'.
        path (str): The base path in the S3 bucket. Defaults to BUCKET_PATH.

        Returns:
        Tuple[str, io.BytesIO, List[ZipInfo]]: A tuple containing the zip name, the zip file as a BytesIO 
		object, and a list of ZipInfo objects.
        """
        self.file_type = file_type
        try:
            self.path = f"{path}{self.name}_{self.file_type.upper()}_{date}.zip"
            s3_object = self._s3_object(self.path)
            zip_name = f"{self.name}_{self.file_type.upper()}_{date}.zip"
        except:
            self.path = f"{path}{self.name}_{self.file_type.upper()}.zip"
            s3_object = self._s3_object(self.path)
            zip_name = f"{self.name}_{self.file_type.upper()}.zip"

        zip_file = io.BytesIO(s3_object["Body"].read())

        result = zip_name, zip_file

        return result

Existem casos em que o self.name pode ser gravado como CENTRAL-NACIONAL ou CENTRAL_NACIONAL 
e também o self.file_type pode ser AJUSTE_MANUAL ou AJUSTE-MANUAL
