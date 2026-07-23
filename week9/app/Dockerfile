# A stand-in "app" image the pipeline builds and pushes. Kept trivial on purpose —
# the capstone point is the build -> push -> announce flow, not the app itself.
FROM alpine:3.19
RUN echo "calculator app" > /app-name
CMD ["cat", "/app-name"]
