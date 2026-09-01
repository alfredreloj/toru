/**
 * One-off: create the Toru Spaces bucket and set a 1-day expiry lifecycle rule.
 * Safe to re-run — CreateBucket on an existing bucket is treated as success.
 */
import "dotenv/config";
import {
  S3Client,
  CreateBucketCommand,
  PutBucketLifecycleConfigurationCommand,
  GetBucketLifecycleConfigurationCommand,
} from "@aws-sdk/client-s3";

const {
  SPACES_KEY,
  SPACES_SECRET,
  SPACES_REGION,
  SPACES_ENDPOINT,
  SPACES_BUCKET,
} = process.env;

const s3 = new S3Client({
  region: SPACES_REGION,
  endpoint: SPACES_ENDPOINT,
  credentials: { accessKeyId: SPACES_KEY, secretAccessKey: SPACES_SECRET },
  forcePathStyle: false,
});

async function main() {
  try {
    await s3.send(new CreateBucketCommand({ Bucket: SPACES_BUCKET }));
    console.log(`Created bucket ${SPACES_BUCKET}`);
  } catch (e) {
    if (["BucketAlreadyOwnedByYou", "BucketAlreadyExists"].includes(e.name)) {
      console.log(`Bucket ${SPACES_BUCKET} already exists — ok`);
    } else {
      throw e;
    }
  }

  // Objects under the whole bucket expire 1 day after creation. Toru also tracks
  // expiry in meta.json for the UI, but this rule is the authoritative delete.
  await s3.send(
    new PutBucketLifecycleConfigurationCommand({
      Bucket: SPACES_BUCKET,
      LifecycleConfiguration: {
        Rules: [
          {
            ID: "toru-expire-1-day",
            Status: "Enabled",
            Filter: { Prefix: "" },
            Expiration: { Days: 1 },
          },
        ],
      },
    })
  );
  console.log("Lifecycle rule set: expire all objects after 1 day");

  const check = await s3.send(
    new GetBucketLifecycleConfigurationCommand({ Bucket: SPACES_BUCKET })
  );
  console.log("Verified rules:", JSON.stringify(check.Rules));
}

main().catch((e) => {
  console.error("Setup failed:", e.name, e.message);
  process.exit(1);
});
