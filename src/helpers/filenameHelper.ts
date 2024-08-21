import { makeSlug } from "../utils/app.utils";
import { UploadeFileDataInput } from "../validations/uploadFileData";


const fileNameHelper = async(fileData: UploadeFileDataInput) => {

    const fileName = fileData.original_name;
    let [fileOriginalName, fileExtension] = fileName.split('.');

    // Remove spaces and special characters from the fileOriginalName
    fileOriginalName = makeSlug(fileOriginalName);

    // Get the current date and format it
    const currentDate = new Date();
    const formattedDate = currentDate.toISOString().split('T')[0];

    // Get the current time and format it (HHmmss)
    const formattedTime = currentDate.toTimeString().split(' ')[0].replace(/:/g, '');

    // Construct the unique filename with date and time
    const uniqueFileName = `${formattedDate}_${formattedTime}_${fileOriginalName}.${fileExtension}`;

    return uniqueFileName;
}

export { fileNameHelper };
