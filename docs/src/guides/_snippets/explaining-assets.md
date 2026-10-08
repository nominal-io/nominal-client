In Nominal, an asset is the digital representation of a physical device operated during tests.
For example, if you are handling data for a fleet of airplanes, then each of those airplanes could be represented as an asset within Nominal.

:::{tip}

While it is simple to describe an asset as a 1:1 relation between physical assets, an asset may also refer to a shared concept.
For example, an asset in Nominal could be used to group simulation runs for a planned future aircraft that doesn't exist physically yet.
:::

Assets have (amongst other things):

* A human readable name and a description.
* Properties which may help programmatically identify an asset from others. Some examples:
    * For airplanes: a model and tail number are commonly used to map between a physical plane and the asset in Nominal.
    * For self-driving food delivery cars: a license plate number, make / model, sensors fitted (`has_lidar`, `has_radar`, `has_night_vision_cameras`, etc.)
    * For vibration stands: a serial number, type, and warehouse number.

Other associated metadata may include labels, URLs to resources associated with the asset, file attachments, etc.
As shown later, an asset can be found by name or by searching metadata.

:::{tip}

Being able to uniquely identify an asset in Nominal via a combination of properties enables easy lookup and search based on conventions contextual to your organization.
:::
