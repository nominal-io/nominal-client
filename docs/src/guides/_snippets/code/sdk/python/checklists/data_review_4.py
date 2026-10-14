review_builder = client.data_review_builder()
review_builder.add_integration(integration_rid)  # Optional

# Add requests for run1
review_builder.add_request(run1_rid, checklist1_rid, checklist1_commit)
review_builder.add_request(run1_rid, checklist2_rid, checklist2_commit)  # Optional

# Add requests for run2
review_builder.add_request(run2_rid, checklist1_rid, checklist1_commit)  # Optional

# Initiate reviews
reviews = review_builder.initiate()

# Print out results
for review in reviews:
    print(f"review: {review.rid}, URL: {review.nominal_url}")
    print(f"  nr. of violations: {len(review.get_violations())}")
