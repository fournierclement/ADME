# ADME
SSP Data science project

# INSTRUCTIONS

Make sure you have scala and sbt installed.
Follow the instructions here and choose “Download SBT” if it isn't the case :
https://www.scala-lang.org/download/

Once you are in the “ADME” directory, enter “sbt” in the command line.

Once you have access to the sbt shell, enter the command "run", followed by the location of your data-students.json
that will be read and used to create the model. For example, if your data-students.json is in a folder "resources" inside the ADME folder,
you should type :
"run resources/data-students.json"

You will have a choice proposed to you.
Type the corresponding number to the one attributed to model.Main and press enter.

Wait a moment, and your model will be created and information will be displayed to you about the model used on a validation set.

Now, if you want to use an already created model, in the sbt shell :
type "run", followed by the path of your data-students.json, the path to your model and the path to the csv file in the output.

For example, once you are in the sbt shell, type :
"run resources/data-students.json randomTreeModel/model.json output/output.csv".

Now, choose the number corresponding to predictor.Main and press enter.

Wait a while for the program to finish, and you will have the csv file created at the desired location.